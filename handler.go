package slogopenobserve

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/samber/lo"
	slogcommon "github.com/samber/slog-common"
	"github.com/valyala/fasthttp"
	"golang.org/x/time/rate"
)

var mapPool = sync.Pool{
	New: func() any {
		return make(map[string]any, 16)
	},
}

const (
	shardCount     = 32
	maxErrorKeyLen = 100
)

type shard struct {
	mu       sync.RWMutex
	limiters map[string]*rateLimiterEntry
}

type Option struct {
	// log level (default: debug)
	Level slog.Leveler

	Endpoint      string
	Username      string
	Password      string
	Organization  string
	Stream        string
	CustomHeaders map[string]string
	Timeout       time.Duration // default: 10s
	channel       chan map[string]any
	NumWorkers    int // default: 4, number of worker goroutines

	// optional: customize webhook event builder
	Converter Converter
	// optional: custom marshaler
	Marshaler func(v any) ([]byte, error)
	// optional: fetch attributes from context
	AttrFromContext []func(ctx context.Context) []slog.Attr

	// optional: see slog.HandlerOptions
	AddSource   bool
	ReplaceAttr func(groups []string, a slog.Attr) slog.Attr

	// ErrorRateLimit settings
	ErrorRateLimit      rate.Limit    // default: 10000 message per 1 seconds
	ErrorRateBurst      int           // default: 10000
	ErrorLimiterTTL     time.Duration // default: 10 minutes - how long to keep unused limiters
	ErrorLimiterCleanup time.Duration // default: 5 minutes - how often to clean up unused limiters
}

// rateLimiterEntry holds a rate limiter along with its last usage time
type rateLimiterEntry struct {
	limiter  *rate.Limiter
	lastUsed time.Time
}

func (o Option) NewOpenobserveHandler() slog.Handler {
	if o.Level == nil {
		o.Level = slog.LevelDebug
	}

	if o.Endpoint != "" {
		o.Endpoint = strings.TrimSuffix(o.Endpoint, "/")
	}

	if o.Timeout == 0 {
		o.Timeout = 10 * time.Second
	}

	if o.Converter == nil {
		o.Converter = DefaultConverter
	}

	if o.Marshaler == nil {
		o.Marshaler = json.Marshal
	}

	if o.AttrFromContext == nil {
		o.AttrFromContext = []func(ctx context.Context) []slog.Attr{}
	}

	if o.NumWorkers <= 0 {
		o.NumWorkers = 4 // Default to 4 workers if not specified or invalid
	}

	// Set defaults for rate limiting
	if o.ErrorRateLimit == 0 {
		o.ErrorRateLimit = rate.Limit(10000)
	}

	if o.ErrorRateBurst <= 0 {
		o.ErrorRateBurst = 10000
	}

	if o.ErrorLimiterTTL == 0 {
		o.ErrorLimiterTTL = 10 * time.Minute
	}

	if o.ErrorLimiterCleanup == 0 {
		o.ErrorLimiterCleanup = 5 * time.Minute
	}

	sendCh := make(chan map[string]any, 100000)
	o.channel = sendCh

	h := &OpenobserveHandler{
		option:      o,
		attrs:       []slog.Attr{},
		groups:      []string{},
		cleanupDone: make(chan struct{}),
		client: &fasthttp.Client{
			Name:                      "slog-openobserve",
			MaxConnsPerHost:           o.NumWorkers + 8,
			MaxIdleConnDuration:       10 * time.Second,
			ReadTimeout:               o.Timeout,
			WriteTimeout:              o.Timeout,
			MaxIdemponentCallAttempts: 1, // Fail fast, don't retry locally
		},
	}

	// Start multiple workers based on configuration
	for i := 0; i < o.NumWorkers; i++ {
		go h.worker(i)
	}

	for i := 0; i < shardCount; i++ {
		h.shards[i] = &shard{
			limiters: make(map[string]*rateLimiterEntry),
		}
	}

	// Start cleanup goroutine
	go h.cleanupUnusedLimiters()

	return h
}

var _ slog.Handler = (*OpenobserveHandler)(nil)

type OpenobserveHandler struct {
	option      Option
	attrs       []slog.Attr
	groups      []string
	shards      [shardCount]*shard
	cleanupDone chan struct{} // Signal channel for cleanup goroutine
	stopOnce    sync.Once
	client      *fasthttp.Client
}

// cleanupUnusedLimiters periodically removes unused rate limiters
func (h *OpenobserveHandler) cleanupUnusedLimiters() {
	ticker := time.NewTicker(h.option.ErrorLimiterCleanup)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			now := time.Now()

			for i := 0; i < shardCount; i++ {
				s := h.shards[i]
				s.mu.Lock()
				// Find limiters that haven't been used recently
				for msg, entry := range s.limiters {
					if now.Sub(entry.lastUsed) > h.option.ErrorLimiterTTL {
						delete(s.limiters, msg)
					}
				}
				s.mu.Unlock()
			}

		case <-h.cleanupDone:
			return
		}
	}
}

func (h *OpenobserveHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.option.Level.Level()
}

func (h *OpenobserveHandler) Handle(ctx context.Context, record slog.Record) error {
	// Apply rate limiting for error logs to prevent channel overflow
	if record.Level >= slog.LevelError {
		errorMsg := record.Message

		if !h.allowMessage(errorMsg) {
			// Skip this message as it's being rate limited
			return nil
		}
	}

	fromContext := slogcommon.ContextExtractor(ctx, h.option.AttrFromContext)
	payload := h.option.Converter(h.option.AddSource, h.option.ReplaceAttr, append(h.attrs, fromContext...), h.groups, &record)
	select {
	case h.option.channel <- payload:
	default:
		// Channel is full, log is dropped
	}
	return nil
}

// allowMessage checks if a message should be allowed based on rate limiting
// allowMessage checks if a message should be allowed based on rate limiting
func (h *OpenobserveHandler) allowMessage(msg string) bool {
	// Truncate message to avoid infinite key growth
	if len(msg) > maxErrorKeyLen {
		msg = msg[:maxErrorKeyLen]
	}

	// Calculate shard
	hasher := fnv.New32a()
	hasher.Write([]byte(msg))
	shardIdx := hasher.Sum32() % uint32(shardCount)
	s := h.shards[shardIdx]

	s.mu.RLock()
	entry, exists := s.limiters[msg]
	s.mu.RUnlock()

	if !exists {
		s.mu.Lock()
		// Check again in case another goroutine created it while we were waiting for the lock
		entry, exists = s.limiters[msg]
		if !exists {
			// Create a new rate limiter for this error type
			limiter := rate.NewLimiter(h.option.ErrorRateLimit, h.option.ErrorRateBurst)
			entry = &rateLimiterEntry{
				limiter:  limiter,
				lastUsed: time.Now(),
			}
			s.limiters[msg] = entry
		} else {
			entry.lastUsed = time.Now()
		}
		s.mu.Unlock()
	} else {
		// Update last used time if the limiter already exists
		s.mu.Lock()
		entry.lastUsed = time.Now()
		s.mu.Unlock()
	}

	// Allow the message if the token bucket has tokens available
	return entry.limiter.Allow()
}

func (h *OpenobserveHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &OpenobserveHandler{
		option:      h.option,
		attrs:       slogcommon.AppendAttrsToGroup(h.groups, h.attrs, attrs...),
		groups:      h.groups,
		shards:      h.shards,
		cleanupDone: h.cleanupDone,
	}
}

func (h *OpenobserveHandler) WithGroup(name string) slog.Handler {
	return &OpenobserveHandler{
		option:      h.option,
		attrs:       h.attrs,
		groups:      append(h.groups, name),
		shards:      h.shards,
		cleanupDone: h.cleanupDone,
	}
}

// Shutdown ensures cleanup resources are properly released
func (h *OpenobserveHandler) Shutdown() {
	h.stopOnce.Do(func() {
		close(h.cleanupDone)
		close(h.option.channel)
	})
}

// worker replaces the async function and processes logs in parallel
func (h *OpenobserveHandler) worker(workerID int) {
	for {
		// read 1k items
		// wait up to 1 second
		items, length, _, ok := lo.BufferWithTimeout(h.option.channel, 1000, 1*time.Second)
		if !ok {
			break
		}

		// empty logs
		if length == 0 {
			continue
		}

		// do batching stuff
		req := fasthttp.AcquireRequest()
		req.Header.SetContentType("application/json")
		req.Header.SetUserAgent(name)
		if h.option.Username != "" && h.option.Password != "" {
			userPass := h.option.Username + ":" + h.option.Password
			b64UserPass := base64.StdEncoding.EncodeToString([]byte(userPass))
			req.Header.Set("Authorization", "Basic "+b64UserPass)
		}
		if len(h.option.CustomHeaders) > 0 {
			for k, v := range h.option.CustomHeaders {
				req.Header.Set(k, v)
			}
		}
		bts, _ := h.option.Marshaler(items)
		req.SetBody(bts)
		req.Header.SetMethod(http.MethodPost)
		endpointUrl := fmt.Sprintf("%s/api/%s/%s/_json", h.option.Endpoint, h.option.Organization, h.option.Stream)
		req.SetRequestURI(endpointUrl)
		res := fasthttp.AcquireResponse()
		if err := h.client.DoTimeout(req, res, h.option.Timeout); err != nil {
			slog.Error("failed to send message to openobserve:",
				slog.Any("error", err),
				slog.Int("workerID", workerID))
		}

		fasthttp.ReleaseRequest(req)
		fasthttp.ReleaseResponse(res)

		// Release maps to pool
		for _, item := range items {
			clear(item)
			mapPool.Put(item)
		}
	}
}
