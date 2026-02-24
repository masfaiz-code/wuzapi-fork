package main

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/rs/zerolog/log"
)

// S3Config holds S3 configuration for a user
type S3Config struct {
	Enabled       bool
	Endpoint      string
	Region        string
	Bucket        string
	AccessKey     string
	SecretKey     string
	PathStyle     bool
	PublicURL     string
	MediaDelivery string
	RetentionDays int
}

// S3Manager manages S3 operations
type S3Manager struct {
	mu                  sync.RWMutex
	clients             map[string]*s3.Client
	configs             map[string]*S3Config
	secondaryClients    map[string]*s3.Client
	secondaryConfigs    map[string]*S3Config
	primaryFailureCount map[string]int
	primaryUnhealthyTil map[string]time.Time
	failoverThreshold   map[string]int
	failoverCooldown    map[string]time.Duration
}

// Global S3 manager instance
var s3Manager = &S3Manager{
	clients:             make(map[string]*s3.Client),
	configs:             make(map[string]*S3Config),
	secondaryClients:    make(map[string]*s3.Client),
	secondaryConfigs:    make(map[string]*S3Config),
	primaryFailureCount: make(map[string]int),
	primaryUnhealthyTil: make(map[string]time.Time),
	failoverThreshold:   make(map[string]int),
	failoverCooldown:    make(map[string]time.Duration),
}

func createS3Client(config *S3Config) *s3.Client {
	// Create custom credentials provider
	credProvider := credentials.NewStaticCredentialsProvider(
		config.AccessKey,
		config.SecretKey,
		"",
	)

	// Configure S3 client
	cfg := aws.Config{
		Region:      config.Region,
		Credentials: credProvider,
	}

	if config.Endpoint != "" {
		customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			if service == s3.ServiceID {
				return aws.Endpoint{
					URL:               config.Endpoint,
					HostnameImmutable: config.PathStyle,
				}, nil
			}
			return aws.Endpoint{}, &aws.EndpointNotFoundError{}
		})
		cfg.EndpointResolverWithOptions = customResolver
	}

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = config.PathStyle
	})
}

// GetS3Manager returns the global S3 manager instance
func GetS3Manager() *S3Manager {
	return s3Manager
}

// InitializeS3Client creates or updates S3 client for a user
func (m *S3Manager) InitializeS3Client(userID string, config *S3Config) error {
	if !config.Enabled {
		m.RemoveClient(userID)
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	client := createS3Client(config)

	m.clients[userID] = client
	m.configs[userID] = config
	m.primaryFailureCount[userID] = 0
	delete(m.primaryUnhealthyTil, userID)
	m.failoverThreshold[userID] = 2
	m.failoverCooldown[userID] = 10 * time.Minute

	log.Info().Str("userID", userID).Str("bucket", config.Bucket).Msg("S3 client initialized")
	return nil
}

func (m *S3Manager) ConfigureFailover(userID string, secondaryConfig *S3Config, threshold int, cooldownMinutes int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if secondaryConfig != nil && secondaryConfig.Enabled {
		m.secondaryClients[userID] = createS3Client(secondaryConfig)
		m.secondaryConfigs[userID] = secondaryConfig
	} else {
		delete(m.secondaryClients, userID)
		delete(m.secondaryConfigs, userID)
	}

	if threshold < 1 {
		threshold = 2
	}
	if cooldownMinutes < 1 {
		cooldownMinutes = 10
	}

	m.failoverThreshold[userID] = threshold
	m.failoverCooldown[userID] = time.Duration(cooldownMinutes) * time.Minute
}

// RemoveClient removes S3 client for a user
func (m *S3Manager) RemoveClient(userID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.clients, userID)
	delete(m.configs, userID)
	delete(m.secondaryClients, userID)
	delete(m.secondaryConfigs, userID)
	delete(m.primaryFailureCount, userID)
	delete(m.primaryUnhealthyTil, userID)
	delete(m.failoverThreshold, userID)
	delete(m.failoverCooldown, userID)
}

// GetClient returns S3 client for a user
func (m *S3Manager) GetClient(userID string) (*s3.Client, *S3Config, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	client, clientOk := m.clients[userID]
	config, configOk := m.configs[userID]

	return client, config, clientOk && configOk
}

func (m *S3Manager) GetSecondaryClient(userID string) (*s3.Client, *S3Config, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	client, clientOk := m.secondaryClients[userID]
	config, configOk := m.secondaryConfigs[userID]

	return client, config, clientOk && configOk
}

// GenerateS3Key generates S3 object key based on message metadata
func (m *S3Manager) GenerateS3Key(userID, contactJID, messageID string, mimeType string, isIncoming bool) string {
	// Determine direction
	direction := "outbox"
	if isIncoming {
		direction = "inbox"
	}

	// Clean contact JID
	contactJID = strings.ReplaceAll(contactJID, "@", "_")
	contactJID = strings.ReplaceAll(contactJID, ":", "_")

	// Get current time
	now := time.Now()
	year := now.Format("2025")
	month := now.Format("05")
	day := now.Format("25")

	// Determine media type folder
	mediaType := "documents"
	if strings.HasPrefix(mimeType, "image/") {
		mediaType = "images"
	} else if strings.HasPrefix(mimeType, "video/") {
		mediaType = "videos"
	} else if strings.HasPrefix(mimeType, "audio/") {
		mediaType = "audio"
	}

	// Get file extension
	ext := ".bin"
	switch {
	case strings.Contains(mimeType, "jpeg"), strings.Contains(mimeType, "jpg"):
		ext = ".jpg"
	case strings.Contains(mimeType, "png"):
		ext = ".png"
	case strings.Contains(mimeType, "gif"):
		ext = ".gif"
	case strings.Contains(mimeType, "webp"):
		ext = ".webp"
	case strings.Contains(mimeType, "mp4"):
		ext = ".mp4"
	case strings.Contains(mimeType, "webm"):
		ext = ".webm"
	case strings.Contains(mimeType, "ogg"):
		ext = ".ogg"
	case strings.Contains(mimeType, "opus"):
		ext = ".opus"
	case strings.Contains(mimeType, "pdf"):
		ext = ".pdf"
	case strings.Contains(mimeType, "doc"):
		if strings.Contains(mimeType, "docx") {
			ext = ".docx"
		} else {
			ext = ".doc"
		}
	}

	// Build S3 key
	key := fmt.Sprintf("users/%s/%s/%s/%s/%s/%s/%s/%s%s",
		userID,
		direction,
		contactJID,
		year,
		month,
		day,
		mediaType,
		messageID,
		ext,
	)

	return key
}

// UploadToS3 uploads file to S3 and returns the key
func (m *S3Manager) UploadToS3(ctx context.Context, userID string, key string, data []byte, mimeType string) error {
	client, config, ok := m.GetClient(userID)
	if !ok {
		return fmt.Errorf("S3 client not initialized for user %s", userID)
	}

	// Set content type and cache headers for preview
	contentType := mimeType
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	// Calculate expiration time based on retention days
	var expires *time.Time
	if config.RetentionDays > 0 {
		expirationTime := time.Now().Add(time.Duration(config.RetentionDays) * 24 * time.Hour)
		expires = &expirationTime
	}

	input := &s3.PutObjectInput{
		Bucket:       aws.String(config.Bucket),
		Key:          aws.String(key),
		Body:         bytes.NewReader(data),
		ContentType:  aws.String(contentType),
		CacheControl: aws.String("public, max-age=3600"),
		ACL:          types.ObjectCannedACLPublicRead,
	}

	if expires != nil {
		input.Expires = expires
	}

	// Add content disposition for inline preview
	if strings.HasPrefix(mimeType, "image/") || strings.HasPrefix(mimeType, "video/") || mimeType == "application/pdf" {
		input.ContentDisposition = aws.String("inline")
	}

	_, err := client.PutObject(ctx, input)
	if err != nil {
		// Some S3-compatible providers (R2/MinIO variants) reject ACL headers.
		errLower := strings.ToLower(err.Error())
		if strings.Contains(errLower, "accesscontrollistnotsupported") ||
			strings.Contains(errLower, "acl") ||
			strings.Contains(errLower, "x-amz-acl") {
			inputNoACL := &s3.PutObjectInput{
				Bucket:       input.Bucket,
				Key:          input.Key,
				Body:         bytes.NewReader(data),
				ContentType:  input.ContentType,
				CacheControl: input.CacheControl,
				Expires:      input.Expires,
			}
			if input.ContentDisposition != nil {
				inputNoACL.ContentDisposition = input.ContentDisposition
			}

			_, retryErr := client.PutObject(ctx, inputNoACL)
			if retryErr == nil {
				log.Warn().Str("userID", userID).Str("bucket", config.Bucket).Msg("S3 upload succeeded after retrying without ACL")
				return nil
			}

			return fmt.Errorf("failed to upload to S3 (with ACL and without ACL): first=%v retry=%w", err, retryErr)
		}

		return fmt.Errorf("failed to upload to S3: %w", err)
	}

	return nil
}

// GetPublicURL generates public URL for S3 object
func (m *S3Manager) GetPublicURL(userID, key string) string {
	_, config, ok := m.GetClient(userID)
	if !ok {
		return ""
	}

	// Use custom public URL if configured
	if config.PublicURL != "" {
		return fmt.Sprintf("%s/%s/%s", strings.TrimRight(config.PublicURL, "/"), config.Bucket, key)
	}

	// Generate standard S3 URL
	if config.PathStyle {
		return fmt.Sprintf("%s/%s/%s",
			strings.TrimRight(config.Endpoint, "/"),
			config.Bucket,
			key)
	}

	// Virtual hosted-style URL
	if strings.Contains(config.Endpoint, "amazonaws.com") {
		return fmt.Sprintf("https://%s.s3.%s.amazonaws.com/%s",
			config.Bucket,
			config.Region,
			key)
	}

	// For other S3-compatible services
	endpoint := strings.TrimPrefix(config.Endpoint, "https://")
	endpoint = strings.TrimPrefix(endpoint, "http://")
	return fmt.Sprintf("https://%s.%s/%s", config.Bucket, endpoint, key)
}

// TestConnection tests S3 connection
func (m *S3Manager) TestConnection(ctx context.Context, userID string) error {
	client, config, ok := m.GetClient(userID)
	if !ok {
		return fmt.Errorf("S3 client not initialized for user %s", userID)
	}

	// Try to list objects with max 1 result
	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(config.Bucket),
		MaxKeys: aws.Int32(1),
	}

	_, err := client.ListObjectsV2(ctx, input)
	return err
}

// ProcessMediaForS3 handles the complete media upload process
func (m *S3Manager) ProcessMediaForS3(ctx context.Context, userID, contactJID, messageID string,
	data []byte, mimeType string, fileName string, isIncoming bool) (map[string]interface{}, error) {

	// Generate S3 key
	key := m.GenerateS3Key(userID, contactJID, messageID, mimeType, isIncoming)

	// Upload to S3
	err := m.UploadToS3(ctx, userID, key, data, mimeType)
	if err != nil {
		return nil, fmt.Errorf("failed to upload to S3: %w", err)
	}

	// Generate public URL
	publicURL := m.GetPublicURL(userID, key)

	// Return S3 metadata
	s3Data := map[string]interface{}{
		"url":      publicURL,
		"key":      key,
		"bucket":   m.configs[userID].Bucket,
		"size":     len(data),
		"mimeType": mimeType,
		"fileName": fileName,
	}

	return s3Data, nil
}

func (m *S3Manager) getFailoverThresholdForUser(userID string) int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	v, ok := m.failoverThreshold[userID]
	if !ok || v < 1 {
		return 2
	}
	return v
}

func (m *S3Manager) getFailoverCooldownForUser(userID string) time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()

	v, ok := m.failoverCooldown[userID]
	if !ok || v < time.Minute {
		return 10 * time.Minute
	}
	return v
}

func isRetryableS3Error(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "timeout") ||
		strings.Contains(s, "tempor") ||
		strings.Contains(s, "connection") ||
		strings.Contains(s, "refused") ||
		strings.Contains(s, "reset") ||
		strings.Contains(s, "unavailable") ||
		strings.Contains(s, "internalerror") ||
		strings.Contains(s, "serviceunavailable") ||
		strings.Contains(s, "throttl") ||
		strings.Contains(s, "permanentredirect")
}

func (m *S3Manager) markPrimaryFailure(userID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.primaryFailureCount[userID] = m.primaryFailureCount[userID] + 1
	if m.primaryFailureCount[userID] >= m.getFailoverThresholdForUser(userID) {
		until := time.Now().Add(m.getFailoverCooldownForUser(userID))
		m.primaryUnhealthyTil[userID] = until
		log.Warn().Str("userID", userID).Time("primary_unhealthy_until", until).Msg("Primary S3 marked unhealthy")
	}
}

func (m *S3Manager) markPrimarySuccess(userID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.primaryFailureCount[userID] = 0
	delete(m.primaryUnhealthyTil, userID)
}

func (m *S3Manager) isPrimaryUnhealthy(userID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	until, ok := m.primaryUnhealthyTil[userID]
	if !ok {
		return false
	}
	return time.Now().Before(until)
}

func (m *S3Manager) ProcessMediaForS3WithFailover(ctx context.Context, userID, contactJID, messageID string,
	data []byte, mimeType string, fileName string, isIncoming bool) (map[string]interface{}, error) {

	useSecondaryFirst := m.isPrimaryUnhealthy(userID)

	tryPrimary := func() (map[string]interface{}, error) {
		result, err := m.ProcessMediaForS3(ctx, userID, contactJID, messageID, data, mimeType, fileName, isIncoming)
		if err != nil {
			if isRetryableS3Error(err) {
				m.markPrimaryFailure(userID)
			}
			return nil, err
		}

		m.markPrimarySuccess(userID)
		result["provider"] = "primary"
		result["failover_used"] = false
		return result, nil
	}

	trySecondary := func(primaryErr error) (map[string]interface{}, error) {
		client, secondaryConfig, ok := m.GetSecondaryClient(userID)
		if !ok {
			if primaryErr != nil {
				return nil, primaryErr
			}
			return nil, fmt.Errorf("secondary S3 client not configured")
		}

		key := m.GenerateS3Key(userID, contactJID, messageID, mimeType, isIncoming)
		if err := m.uploadToClient(ctx, client, secondaryConfig, userID, key, data, mimeType); err != nil {
			if primaryErr != nil {
				return nil, fmt.Errorf("primary failed: %v; secondary failed: %w", primaryErr, err)
			}
			return nil, err
		}

		publicURL := m.getPublicURLFromConfig(secondaryConfig, key)
		return map[string]interface{}{
			"url":           publicURL,
			"key":           key,
			"bucket":        secondaryConfig.Bucket,
			"size":          len(data),
			"mimeType":      mimeType,
			"fileName":      fileName,
			"provider":      "secondary",
			"failover_used": true,
		}, nil
	}

	if useSecondaryFirst {
		if result, err := trySecondary(nil); err == nil {
			return result, nil
		}
		return tryPrimary()
	}

	primaryResult, primaryErr := tryPrimary()
	if primaryErr == nil {
		return primaryResult, nil
	}

	if !isRetryableS3Error(primaryErr) {
		return nil, primaryErr
	}

	return trySecondary(primaryErr)
}

func (m *S3Manager) getPublicURLFromConfig(config *S3Config, key string) string {
	if config.PublicURL != "" {
		return fmt.Sprintf("%s/%s/%s", strings.TrimRight(config.PublicURL, "/"), config.Bucket, key)
	}

	if config.PathStyle {
		return fmt.Sprintf("%s/%s/%s", strings.TrimRight(config.Endpoint, "/"), config.Bucket, key)
	}

	if strings.Contains(config.Endpoint, "amazonaws.com") {
		return fmt.Sprintf("https://%s.s3.%s.amazonaws.com/%s", config.Bucket, config.Region, key)
	}

	endpoint := strings.TrimPrefix(config.Endpoint, "https://")
	endpoint = strings.TrimPrefix(endpoint, "http://")
	return fmt.Sprintf("https://%s.%s/%s", config.Bucket, endpoint, key)
}

func (m *S3Manager) uploadToClient(ctx context.Context, client *s3.Client, config *S3Config, userID string, key string, data []byte, mimeType string) error {
	contentType := mimeType
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	var expires *time.Time
	if config.RetentionDays > 0 {
		expirationTime := time.Now().Add(time.Duration(config.RetentionDays) * 24 * time.Hour)
		expires = &expirationTime
	}

	input := &s3.PutObjectInput{
		Bucket:       aws.String(config.Bucket),
		Key:          aws.String(key),
		Body:         bytes.NewReader(data),
		ContentType:  aws.String(contentType),
		CacheControl: aws.String("public, max-age=3600"),
		ACL:          types.ObjectCannedACLPublicRead,
	}

	if expires != nil {
		input.Expires = expires
	}

	if strings.HasPrefix(mimeType, "image/") || strings.HasPrefix(mimeType, "video/") || mimeType == "application/pdf" {
		input.ContentDisposition = aws.String("inline")
	}

	_, err := client.PutObject(ctx, input)
	if err != nil {
		errLower := strings.ToLower(err.Error())
		if strings.Contains(errLower, "accesscontrollistnotsupported") ||
			strings.Contains(errLower, "acl") ||
			strings.Contains(errLower, "x-amz-acl") {
			inputNoACL := &s3.PutObjectInput{
				Bucket:       input.Bucket,
				Key:          input.Key,
				Body:         bytes.NewReader(data),
				ContentType:  input.ContentType,
				CacheControl: input.CacheControl,
				Expires:      input.Expires,
			}
			if input.ContentDisposition != nil {
				inputNoACL.ContentDisposition = input.ContentDisposition
			}

			_, retryErr := client.PutObject(ctx, inputNoACL)
			if retryErr == nil {
				log.Warn().Str("userID", userID).Str("bucket", config.Bucket).Msg("S3 upload succeeded after retrying without ACL")
				return nil
			}

			return fmt.Errorf("failed to upload to S3 (with ACL and without ACL): first=%v retry=%w", err, retryErr)
		}

		return fmt.Errorf("failed to upload to S3: %w", err)
	}

	return nil
}

// DeleteAllUserObjects deletes all user files from S3
func (m *S3Manager) DeleteAllUserObjects(ctx context.Context, userID string) error {
	client, config, ok := m.GetClient(userID)
	if !ok {
		return fmt.Errorf("S3 client not initialized for user %s", userID)
	}

	prefix := fmt.Sprintf("users/%s/", userID)
	var toDelete []types.ObjectIdentifier
	var continuationToken *string

	for {
		input := &s3.ListObjectsV2Input{
			Bucket:            aws.String(config.Bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: continuationToken,
		}
		output, err := client.ListObjectsV2(ctx, input)
		if err != nil {
			return fmt.Errorf("failed to list objects for user %s: %w", userID, err)
		}

		for _, obj := range output.Contents {
			toDelete = append(toDelete, types.ObjectIdentifier{Key: obj.Key})
			// Delete in batches of 1000 (S3 limit)
			if len(toDelete) == 1000 {
				_, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
					Bucket: aws.String(config.Bucket),
					Delete: &types.Delete{Objects: toDelete},
				})
				if err != nil {
					return fmt.Errorf("failed to delete objects for user %s: %w", userID, err)
				}
				toDelete = nil
			}
		}

		if output.IsTruncated != nil && *output.IsTruncated && output.NextContinuationToken != nil {
			continuationToken = output.NextContinuationToken
		} else {
			break
		}
	}

	// Delete any remaining objects
	if len(toDelete) > 0 {
		_, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(config.Bucket),
			Delete: &types.Delete{Objects: toDelete},
		})
		if err != nil {
			return fmt.Errorf("failed to delete objects for user %s: %w", userID, err)
		}
	}

	log.Info().Str("userID", userID).Msg("all user files removed from S3")
	return nil
}
