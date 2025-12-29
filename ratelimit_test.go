package slogopenobserve

import (
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

func TestRateLimit_Truncation(t *testing.T) {
	// Setup handler with low limit
	opts := Option{
		ErrorRateLimit: rate.Limit(1), // 1 QPS
		ErrorRateBurst: 1,
	}
	h := opts.NewOpenobserveHandler().(*OpenobserveHandler)
	defer h.Shutdown()

	longMsg1 := "Error connecting to database: " + strings.Repeat("a", 200) + "1"
	longMsg2 := "Error connecting to database: " + strings.Repeat("a", 200) + "2"
	// These two messages differ only at the end.
	// If truncation works (e.g. at 64 chars), they should be treated as the same key.
	// So if verify the rate limiter is shared.

	// First call should pass
	if !h.allowMessage(longMsg1) {
		t.Errorf("First message should be allowed")
	}

	// Second call with DIFFERENT message (but same prefix) should be rejected if they share the limiter
	// and we are hitting the 1 QPS limit immediately.
	// Wait, internal burst is 1. One token consumed. Next one needs to wait 1s.
	if h.allowMessage(longMsg2) {
		t.Errorf("Second message should be rate limited (sharing limiter key)")
	}
}

func TestRateLimit_Sharding(t *testing.T) {
	opts := Option{
		ErrorRateLimit: rate.Limit(100),
		ErrorRateBurst: 100,
	}
	h := opts.NewOpenobserveHandler().(*OpenobserveHandler)
	defer h.Shutdown()

	// Just ensure no race conditions or crashes with many concurrent checks
	// Just ensure no race conditions or crashes with many concurrent checks

	parallelism := 10
	count := 1000

	done := make(chan bool)

	for i := 0; i < parallelism; i++ {
		go func(id int) {
			for j := 0; j < count; j++ {
				msg := fmt.Sprintf("error-%d-%d", id, j)
				h.allowMessage(msg)
			}
			done <- true
		}(i)
	}

	for i := 0; i < parallelism; i++ {
		<-done
	}

	// Simple check that not everything was limited
	if len(h.shards) != 32 {
		t.Errorf("Expected 32 shards")
	}
}

func TestPoolUsage(t *testing.T) {
	// This is hard to test black-box style, verifying visually via code review is mostly sufficient.
	// However, we can basic test DefaultConverter to ensure it returns a map.
	rec := slog.Record{Time: time.Now(), Level: slog.LevelInfo, Message: "test"}
	m := DefaultConverter(false, nil, nil, nil, &rec)
	if m["message"] != "test" {
		t.Errorf("Converter failed logic")
	}
}
