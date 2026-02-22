package main

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/rs/zerolog/log"
)

const (
	webhookRetryPolicyExponential = "exponential"
	webhookRetryPolicyLinear      = "linear"
	webhookRetryPolicyConstant    = "constant"
)

type WebhookHeader struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type WebhookHmacPayload struct {
	Key string `json:"key,omitempty"`
}

type WebhookRetriesPayload struct {
	Policy       string `json:"policy,omitempty"`
	DelaySeconds int    `json:"delaySeconds,omitempty"`
	Attempts     int    `json:"attempts,omitempty"`
}

type WebhookPayload struct {
	URL           string                 `json:"url"`
	Events        []string               `json:"events,omitempty"`
	Active        *bool                  `json:"active,omitempty"`
	Hmac          *WebhookHmacPayload    `json:"hmac,omitempty"`
	Retries       *WebhookRetriesPayload `json:"retries,omitempty"`
	CustomHeaders []WebhookHeader        `json:"customHeaders,omitempty"`
}

type UserWebhook struct {
	ID                string       `db:"id" json:"id"`
	UserID            string       `db:"user_id" json:"-"`
	URL               string       `db:"url" json:"url"`
	Events            string       `db:"events" json:"-"`
	Active            bool         `db:"active" json:"active"`
	HmacKey           []byte       `db:"hmac_key" json:"-"`
	RetryPolicy       string       `db:"retry_policy" json:"-"`
	RetryDelaySeconds int          `db:"retry_delay_seconds" json:"-"`
	RetryAttempts     int          `db:"retry_attempts" json:"-"`
	CustomHeadersJSON string       `db:"custom_headers" json:"-"`
	IsPrimaryLegacy   bool         `db:"is_primary_legacy" json:"isPrimaryLegacy"`
	CreatedAt         sql.NullTime `db:"created_at" json:"-"`
	UpdatedAt         sql.NullTime `db:"updated_at" json:"-"`
}

type WebhookResponse struct {
	ID              string                `json:"id"`
	URL             string                `json:"url"`
	Events          []string              `json:"events"`
	Active          bool                  `json:"active"`
	HmacConfigured  bool                  `json:"hmacConfigured"`
	Retries         WebhookRetriesPayload `json:"retries"`
	CustomHeaders   []WebhookHeader       `json:"customHeaders"`
	IsPrimaryLegacy bool                  `json:"isPrimaryLegacy"`
	CreatedAt       string                `json:"createdAt,omitempty"`
	UpdatedAt       string                `json:"updatedAt,omitempty"`
}

type WebhookDispatchConfig struct {
	ID                string
	URL               string
	Events            []string
	Active            bool
	EncryptedHmacKey  []byte
	RetryPolicy       string
	RetryDelaySeconds int
	RetryAttempts     int
	CustomHeaders     map[string]string
	IsPrimaryLegacy   bool
}

func splitEventsCSV(events string) []string {
	if strings.TrimSpace(events) == "" {
		return []string{}
	}

	parts := strings.Split(events, ",")
	seen := map[string]struct{}{}
	out := make([]string, 0, len(parts))

	for _, p := range parts {
		e := strings.TrimSpace(p)
		if e == "" {
			continue
		}
		if _, ok := seen[e]; ok {
			continue
		}
		seen[e] = struct{}{}
		out = append(out, e)
	}

	return out
}

func normalizeAndValidateEvents(events []string) ([]string, error) {
	if len(events) == 0 {
		return []string{}, nil
	}

	seen := map[string]struct{}{}
	valid := make([]string, 0, len(events))

	for _, raw := range events {
		e := strings.TrimSpace(raw)
		if e == "" {
			continue
		}
		if !Find(supportedEventTypes, e) {
			return nil, fmt.Errorf("invalid event type: %s", e)
		}
		if _, ok := seen[e]; ok {
			continue
		}
		seen[e] = struct{}{}
		valid = append(valid, e)
	}

	return valid, nil
}

func eventsToCSV(events []string) string {
	if len(events) == 0 {
		return ""
	}
	return strings.Join(events, ",")
}

func normalizeRetryPolicy(policy string) string {
	p := strings.ToLower(strings.TrimSpace(policy))
	switch p {
	case webhookRetryPolicyConstant, webhookRetryPolicyLinear, webhookRetryPolicyExponential:
		return p
	default:
		return webhookRetryPolicyExponential
	}
}

func validateRetriesPayload(retries *WebhookRetriesPayload) (WebhookRetriesPayload, error) {
	defaults := WebhookRetriesPayload{
		Policy:       normalizeRetryPolicy(""),
		DelaySeconds: *webhookRetryDelaySeconds,
		Attempts:     *webhookRetryCount,
	}

	if retries == nil {
		return defaults, nil
	}

	policy := normalizeRetryPolicy(retries.Policy)
	delay := retries.DelaySeconds
	attempts := retries.Attempts

	if delay <= 0 {
		delay = *webhookRetryDelaySeconds
	}
	if attempts <= 0 {
		attempts = *webhookRetryCount
	}

	if delay > 86400 {
		return WebhookRetriesPayload{}, fmt.Errorf("delaySeconds must be <= 86400")
	}
	if attempts > 20 {
		return WebhookRetriesPayload{}, fmt.Errorf("attempts must be <= 20")
	}

	return WebhookRetriesPayload{Policy: policy, DelaySeconds: delay, Attempts: attempts}, nil
}

func normalizeHeaders(headers []WebhookHeader) []WebhookHeader {
	if len(headers) == 0 {
		return []WebhookHeader{}
	}

	latest := make(map[string]string)
	order := make([]string, 0, len(headers))

	for _, h := range headers {
		name := strings.TrimSpace(h.Name)
		if name == "" {
			continue
		}
		value := strings.TrimSpace(h.Value)
		lower := strings.ToLower(name)
		if _, exists := latest[lower]; !exists {
			order = append(order, lower)
		}
		latest[lower] = value
	}

	out := make([]WebhookHeader, 0, len(order))
	for _, lower := range order {
		out = append(out, WebhookHeader{Name: lower, Value: latest[lower]})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func headersToJSON(headers []WebhookHeader) (string, error) {
	normalized := normalizeHeaders(headers)
	if len(normalized) == 0 {
		return "[]", nil
	}
	b, err := json.Marshal(normalized)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func headersFromJSON(raw string) []WebhookHeader {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return []WebhookHeader{}
	}
	var headers []WebhookHeader
	if err := json.Unmarshal([]byte(raw), &headers); err != nil {
		return []WebhookHeader{}
	}
	return normalizeHeaders(headers)
}

func headersSliceToMap(headers []WebhookHeader) map[string]string {
	m := make(map[string]string)
	for _, h := range headers {
		if strings.TrimSpace(h.Name) == "" {
			continue
		}
		m[strings.TrimSpace(h.Name)] = strings.TrimSpace(h.Value)
	}
	return m
}

func userWebhookToResponse(w UserWebhook) WebhookResponse {
	res := WebhookResponse{
		ID:              w.ID,
		URL:             w.URL,
		Events:          splitEventsCSV(w.Events),
		Active:          w.Active,
		HmacConfigured:  len(w.HmacKey) > 0,
		Retries:         WebhookRetriesPayload{Policy: normalizeRetryPolicy(w.RetryPolicy), DelaySeconds: w.RetryDelaySeconds, Attempts: w.RetryAttempts},
		CustomHeaders:   headersFromJSON(w.CustomHeadersJSON),
		IsPrimaryLegacy: w.IsPrimaryLegacy,
	}

	if w.CreatedAt.Valid {
		res.CreatedAt = w.CreatedAt.Time.UTC().Format(time.RFC3339)
	}
	if w.UpdatedAt.Valid {
		res.UpdatedAt = w.UpdatedAt.Time.UTC().Format(time.RFC3339)
	}

	if res.Retries.DelaySeconds <= 0 {
		res.Retries.DelaySeconds = *webhookRetryDelaySeconds
	}
	if res.Retries.Attempts <= 0 {
		res.Retries.Attempts = *webhookRetryCount
	}
	res.Retries.Policy = normalizeRetryPolicy(res.Retries.Policy)

	return res
}

func dbNowExpression(driver string) string {
	if driver == "sqlite" {
		return "datetime('now')"
	}
	return "NOW()"
}

func dbBoolValue(v bool, driver string) interface{} {
	if driver == "sqlite" {
		if v {
			return 1
		}
		return 0
	}
	return v
}

func sqliteRebind(query string, maxIndex int) string {
	for i := maxIndex; i >= 1; i-- {
		query = strings.ReplaceAll(query, "$"+strconv.Itoa(i), "?")
	}
	return query
}

func (s *server) getUserWebhooks(userID string) ([]UserWebhook, error) {
	query := `
		SELECT id, user_id, url, events, active,
		       COALESCE(hmac_key, '') AS hmac_key,
		       COALESCE(retry_policy, '') AS retry_policy,
		       COALESCE(retry_delay_seconds, 0) AS retry_delay_seconds,
		       COALESCE(retry_attempts, 0) AS retry_attempts,
		       COALESCE(custom_headers, '[]') AS custom_headers,
		       COALESCE(is_primary_legacy, false) AS is_primary_legacy,
		       created_at, updated_at
		FROM user_webhooks
		WHERE user_id = $1
		ORDER BY is_primary_legacy DESC, created_at ASC, id ASC`
	if s.db.DriverName() == "sqlite" {
		query = sqliteRebind(query, 1)
	}

	var hooks []UserWebhook
	if err := s.db.Select(&hooks, query, userID); err != nil {
		return nil, err
	}
	return hooks, nil
}

func (s *server) getWebhookByID(userID, webhookID string) (*UserWebhook, error) {
	query := `
		SELECT id, user_id, url, events, active,
		       COALESCE(hmac_key, '') AS hmac_key,
		       COALESCE(retry_policy, '') AS retry_policy,
		       COALESCE(retry_delay_seconds, 0) AS retry_delay_seconds,
		       COALESCE(retry_attempts, 0) AS retry_attempts,
		       COALESCE(custom_headers, '[]') AS custom_headers,
		       COALESCE(is_primary_legacy, false) AS is_primary_legacy,
		       created_at, updated_at
		FROM user_webhooks
		WHERE id = $1 AND user_id = $2
		LIMIT 1`
	if s.db.DriverName() == "sqlite" {
		query = sqliteRebind(query, 2)
	}

	var hook UserWebhook
	if err := s.db.Get(&hook, query, webhookID, userID); err != nil {
		return nil, err
	}
	return &hook, nil
}

func (s *server) getPrimaryLegacyWebhook(userID string) (*UserWebhook, error) {
	query := `
		SELECT id, user_id, url, events, active,
		       COALESCE(hmac_key, '') AS hmac_key,
		       COALESCE(retry_policy, '') AS retry_policy,
		       COALESCE(retry_delay_seconds, 0) AS retry_delay_seconds,
		       COALESCE(retry_attempts, 0) AS retry_attempts,
		       COALESCE(custom_headers, '[]') AS custom_headers,
		       COALESCE(is_primary_legacy, false) AS is_primary_legacy,
		       created_at, updated_at
		FROM user_webhooks
		WHERE user_id = $1 AND is_primary_legacy = 1
		LIMIT 1`
	if s.db.DriverName() == "sqlite" {
		query = sqliteRebind(query, 1)
	}

	var hook UserWebhook
	if err := s.db.Get(&hook, query, userID); err != nil {
		return nil, err
	}
	return &hook, nil
}

func (s *server) ensurePrimaryLegacyWebhook(userID string) (*UserWebhook, error) {
	hook, err := s.getPrimaryLegacyWebhook(userID)
	if err == nil {
		return hook, nil
	}
	if err != sql.ErrNoRows {
		return nil, err
	}

	var legacyURL string
	var legacyEvents string
	query := "SELECT webhook, events FROM users WHERE id = $1 LIMIT 1"
	if s.db.DriverName() == "sqlite" {
		query = sqliteRebind(query, 1)
	}
	if err := s.db.QueryRow(query, userID).Scan(&legacyURL, &legacyEvents); err != nil {
		return nil, err
	}

	id, err := GenerateRandomID()
	if err != nil {
		return nil, err
	}

	nowExpr := dbNowExpression(s.db.DriverName())
	insert := "INSERT INTO user_webhooks (id, user_id, url, events, active, retry_policy, retry_delay_seconds, retry_attempts, custom_headers, is_primary_legacy, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, " + nowExpr + ", " + nowExpr + ")"
	if s.db.DriverName() == "sqlite" {
		insert = sqliteRebind(insert, 10)
	}

	active := strings.TrimSpace(legacyURL) != ""
	_, err = s.db.Exec(insert,
		id,
		userID,
		legacyURL,
		legacyEvents,
		dbBoolValue(active, s.db.DriverName()),
		webhookRetryPolicyExponential,
		*webhookRetryDelaySeconds,
		*webhookRetryCount,
		"[]",
		dbBoolValue(true, s.db.DriverName()),
	)
	if err != nil {
		return nil, err
	}

	return s.getPrimaryLegacyWebhook(userID)
}

func computeUserEventUnion(webhooks []UserWebhook) []string {
	seen := map[string]struct{}{}
	union := []string{}

	for _, hook := range webhooks {
		if !hook.Active {
			continue
		}
		for _, e := range splitEventsCSV(hook.Events) {
			if _, ok := seen[e]; ok {
				continue
			}
			seen[e] = struct{}{}
			union = append(union, e)
		}
	}

	if _, ok := seen["All"]; ok {
		return []string{"All"}
	}

	sort.Strings(union)
	return union
}

func (s *server) refreshUserWebhookCacheAndSubscriptions(userID, token string) {
	webhooks, err := s.getUserWebhooks(userID)
	if err != nil {
		log.Warn().Err(err).Str("userID", userID).Msg("Failed to refresh webhooks from DB")
		return
	}

	legacyURL := ""
	for _, hook := range webhooks {
		if hook.IsPrimaryLegacy {
			legacyURL = hook.URL
			break
		}
	}

	unionEvents := computeUserEventUnion(webhooks)
	unionEventsCSV := eventsToCSV(unionEvents)

	// Keep legacy users table synchronized for compatibility
	updateUsersSQL := "UPDATE users SET webhook=$1, events=$2 WHERE id=$3"
	if s.db.DriverName() == "sqlite" {
		updateUsersSQL = sqliteRebind(updateUsersSQL, 3)
	}
	if _, err := s.db.Exec(updateUsersSQL, legacyURL, unionEventsCSV, userID); err != nil {
		log.Warn().Err(err).Str("userID", userID).Msg("Failed to sync legacy users webhook/events")
	}

	if cachedUserInfo, found := userinfocache.Get(token); found {
		updatedUserInfo := cachedUserInfo.(Values)
		updatedUserInfo = updateUserInfo(updatedUserInfo, "Webhook", legacyURL).(Values)
		updatedUserInfo = updateUserInfo(updatedUserInfo, "Events", unionEventsCSV).(Values)
		userinfocache.Set(token, updatedUserInfo, cache.NoExpiration)
	}

	clientManager.UpdateMyClientSubscriptions(userID, unionEvents)
	log.Info().Str("user", userID).Strs("events", unionEvents).Msg("Refreshed union webhook subscriptions")
}

func (s *server) listDispatchWebhooks(userID, token string) ([]WebhookDispatchConfig, error) {
	webhooks, err := s.getUserWebhooks(userID)
	if err != nil {
		return nil, err
	}

	var defaultEncryptedHmacKey []byte
	if userinfo, found := userinfocache.Get(token); found {
		encryptedB64 := userinfo.(Values).Get("HmacKeyEncrypted")
		if encryptedB64 != "" {
			defaultEncryptedHmacKey, _ = base64.StdEncoding.DecodeString(encryptedB64)
		}
	}

	configs := make([]WebhookDispatchConfig, 0, len(webhooks))
	for _, w := range webhooks {
		hmacKey := w.HmacKey
		if len(hmacKey) == 0 && len(defaultEncryptedHmacKey) > 0 {
			hmacKey = defaultEncryptedHmacKey
		}

		headers := headersSliceToMap(headersFromJSON(w.CustomHeadersJSON))

		cfg := WebhookDispatchConfig{
			ID:                w.ID,
			URL:               strings.TrimSpace(w.URL),
			Events:            splitEventsCSV(w.Events),
			Active:            w.Active,
			EncryptedHmacKey:  hmacKey,
			RetryPolicy:       normalizeRetryPolicy(w.RetryPolicy),
			RetryDelaySeconds: w.RetryDelaySeconds,
			RetryAttempts:     w.RetryAttempts,
			CustomHeaders:     headers,
			IsPrimaryLegacy:   w.IsPrimaryLegacy,
		}

		if cfg.RetryDelaySeconds <= 0 {
			cfg.RetryDelaySeconds = *webhookRetryDelaySeconds
		}
		if cfg.RetryAttempts <= 0 {
			cfg.RetryAttempts = *webhookRetryCount
		}

		configs = append(configs, cfg)
	}

	return configs, nil
}

func (s *server) upsertPrimaryLegacyWebhook(userID string, payload WebhookPayload) (*UserWebhook, error) {
	hook, err := s.ensurePrimaryLegacyWebhook(userID)
	if err != nil {
		return nil, err
	}

	active := true
	if payload.Active != nil {
		active = *payload.Active
	} else {
		active = strings.TrimSpace(payload.URL) != ""
	}

	events, err := normalizeAndValidateEvents(payload.Events)
	if err != nil {
		return nil, err
	}
	if !active {
		events = []string{}
	}

	retries, err := validateRetriesPayload(payload.Retries)
	if err != nil {
		return nil, err
	}
	headersJSON, err := headersToJSON(payload.CustomHeaders)
	if err != nil {
		return nil, err
	}

	var encryptedHmacKey []byte
	if payload.Hmac != nil && strings.TrimSpace(payload.Hmac.Key) != "" {
		if len(strings.TrimSpace(payload.Hmac.Key)) < 32 {
			return nil, fmt.Errorf("hmac key must be at least 32 characters long")
		}
		encryptedHmacKey, err = encryptHMACKey(strings.TrimSpace(payload.Hmac.Key))
		if err != nil {
			return nil, err
		}
	} else {
		encryptedHmacKey = hook.HmacKey
	}

	nowExpr := dbNowExpression(s.db.DriverName())
	query := "UPDATE user_webhooks SET url=$1, events=$2, active=$3, hmac_key=$4, retry_policy=$5, retry_delay_seconds=$6, retry_attempts=$7, custom_headers=$8, updated_at=" + nowExpr + " WHERE id=$9 AND user_id=$10"
	if s.db.DriverName() == "sqlite" {
		query = sqliteRebind(query, 10)
	}

	_, err = s.db.Exec(
		query,
		strings.TrimSpace(payload.URL),
		eventsToCSV(events),
		dbBoolValue(active, s.db.DriverName()),
		encryptedHmacKey,
		retries.Policy,
		retries.DelaySeconds,
		retries.Attempts,
		headersJSON,
		hook.ID,
		userID,
	)
	if err != nil {
		return nil, err
	}

	return s.getWebhookByID(userID, hook.ID)
}

func (s *server) createWebhook(userID string, payload WebhookPayload) (*UserWebhook, error) {
	if strings.TrimSpace(payload.URL) == "" {
		return nil, fmt.Errorf("url is required")
	}

	events, err := normalizeAndValidateEvents(payload.Events)
	if err != nil {
		return nil, err
	}

	active := true
	if payload.Active != nil {
		active = *payload.Active
	}
	if !active {
		events = []string{}
	}

	retries, err := validateRetriesPayload(payload.Retries)
	if err != nil {
		return nil, err
	}

	headersJSON, err := headersToJSON(payload.CustomHeaders)
	if err != nil {
		return nil, err
	}

	var encryptedHmacKey []byte
	if payload.Hmac != nil && strings.TrimSpace(payload.Hmac.Key) != "" {
		if len(strings.TrimSpace(payload.Hmac.Key)) < 32 {
			return nil, fmt.Errorf("hmac key must be at least 32 characters long")
		}
		encryptedHmacKey, err = encryptHMACKey(strings.TrimSpace(payload.Hmac.Key))
		if err != nil {
			return nil, err
		}
	}

	id, err := GenerateRandomID()
	if err != nil {
		return nil, err
	}

	nowExpr := dbNowExpression(s.db.DriverName())
	query := "INSERT INTO user_webhooks (id, user_id, url, events, active, hmac_key, retry_policy, retry_delay_seconds, retry_attempts, custom_headers, is_primary_legacy, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, " + nowExpr + ", " + nowExpr + ")"
	if s.db.DriverName() == "sqlite" {
		query = sqliteRebind(query, 11)
	}

	_, err = s.db.Exec(
		query,
		id,
		userID,
		strings.TrimSpace(payload.URL),
		eventsToCSV(events),
		dbBoolValue(active, s.db.DriverName()),
		encryptedHmacKey,
		retries.Policy,
		retries.DelaySeconds,
		retries.Attempts,
		headersJSON,
		dbBoolValue(false, s.db.DriverName()),
	)
	if err != nil {
		return nil, err
	}

	return s.getWebhookByID(userID, id)
}

func (s *server) updateWebhook(userID, webhookID string, payload WebhookPayload) (*UserWebhook, error) {
	hook, err := s.getWebhookByID(userID, webhookID)
	if err != nil {
		return nil, err
	}

	urlValue := hook.URL
	if strings.TrimSpace(payload.URL) != "" {
		urlValue = strings.TrimSpace(payload.URL)
	}

	events := splitEventsCSV(hook.Events)
	if payload.Events != nil {
		events, err = normalizeAndValidateEvents(payload.Events)
		if err != nil {
			return nil, err
		}
	}

	active := hook.Active
	if payload.Active != nil {
		active = *payload.Active
	}
	if !active {
		events = []string{}
	}

	retries := WebhookRetriesPayload{
		Policy:       normalizeRetryPolicy(hook.RetryPolicy),
		DelaySeconds: hook.RetryDelaySeconds,
		Attempts:     hook.RetryAttempts,
	}
	if retries.DelaySeconds <= 0 {
		retries.DelaySeconds = *webhookRetryDelaySeconds
	}
	if retries.Attempts <= 0 {
		retries.Attempts = *webhookRetryCount
	}
	if payload.Retries != nil {
		retries, err = validateRetriesPayload(payload.Retries)
		if err != nil {
			return nil, err
		}
	}

	headersJSON := hook.CustomHeadersJSON
	if payload.CustomHeaders != nil {
		headersJSON, err = headersToJSON(payload.CustomHeaders)
		if err != nil {
			return nil, err
		}
	}

	encryptedHmacKey := hook.HmacKey
	if payload.Hmac != nil {
		if strings.TrimSpace(payload.Hmac.Key) == "" {
			encryptedHmacKey = nil
		} else {
			if len(strings.TrimSpace(payload.Hmac.Key)) < 32 {
				return nil, fmt.Errorf("hmac key must be at least 32 characters long")
			}
			encryptedHmacKey, err = encryptHMACKey(strings.TrimSpace(payload.Hmac.Key))
			if err != nil {
				return nil, err
			}
		}
	}

	nowExpr := dbNowExpression(s.db.DriverName())
	query := "UPDATE user_webhooks SET url=$1, events=$2, active=$3, hmac_key=$4, retry_policy=$5, retry_delay_seconds=$6, retry_attempts=$7, custom_headers=$8, updated_at=" + nowExpr + " WHERE id=$9 AND user_id=$10"
	if s.db.DriverName() == "sqlite" {
		query = sqliteRebind(query, 10)
	}

	_, err = s.db.Exec(
		query,
		urlValue,
		eventsToCSV(events),
		dbBoolValue(active, s.db.DriverName()),
		encryptedHmacKey,
		retries.Policy,
		retries.DelaySeconds,
		retries.Attempts,
		headersJSON,
		webhookID,
		userID,
	)
	if err != nil {
		return nil, err
	}

	return s.getWebhookByID(userID, webhookID)
}

func (s *server) deleteWebhookByID(userID, webhookID string) error {
	query := "DELETE FROM user_webhooks WHERE id=$1 AND user_id=$2"
	if s.db.DriverName() == "sqlite" {
		query = sqliteRebind(query, 2)
	}
	res, err := s.db.Exec(query, webhookID, userID)
	if err != nil {
		return err
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return sql.ErrNoRows
	}
	return nil
}
