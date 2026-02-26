package main

import (
	"testing"
	"time"
)

func TestConfigureFailoverDefaults(t *testing.T) {
	manager := &S3Manager{
		failoverThreshold: make(map[string]int),
		failoverCooldown:  make(map[string]time.Duration),
	}

	manager.ConfigureFailover("u1", nil, 0, 0)

	if got := manager.getFailoverThresholdForUser("u1"); got != 2 {
		t.Fatalf("expected default threshold 2, got %d", got)
	}

	if got := manager.getFailoverCooldownForUser("u1"); got != 10*time.Minute {
		t.Fatalf("expected default cooldown 10m, got %v", got)
	}
}
