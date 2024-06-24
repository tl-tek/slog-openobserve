package slogopenobserve

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"time"

	"log/slog"

	slogcommon "github.com/samber/slog-common"
)

type Option struct {
	// log level (default: debug)
	Level slog.Leveler

	// URL
	Endpoint string
	Method   string
	Header   http.Header
	Timeout  time.Duration // default: 10s

	// optional: customize webhook event builder
	Converter Converter
	// optional: custom marshaler
	Marshaler func(v any) ([]byte, error)
	// optional: fetch attributes from context
	AttrFromContext []func(ctx context.Context) []slog.Attr

	// optional: see slog.HandlerOptions
	AddSource   bool
	ReplaceAttr func(groups []string, a slog.Attr) slog.Attr
}

func (o Option) NewOpenobserveHandler() slog.Handler {
	if o.Level == nil {
		o.Level = slog.LevelDebug
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

	return &OpenobserveHandler{
		option: o,
		attrs:  []slog.Attr{},
		groups: []string{},
	}
}

var _ slog.Handler = (*OpenobserveHandler)(nil)

type OpenobserveHandler struct {
	option Option
	attrs  []slog.Attr
	groups []string
}

func (h *OpenobserveHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.option.Level.Level()
}

func (h *OpenobserveHandler) Handle(ctx context.Context, record slog.Record) error {
	fromContext := slogcommon.ContextExtractor(ctx, h.option.AttrFromContext)
	payload := h.option.Converter(h.option.AddSource, h.option.ReplaceAttr, append(h.attrs, fromContext...), h.groups, &record)

	go func() {
		_ = send(h.option.Endpoint, h.option.Method, h.option.Header, h.option.Timeout, h.option.Marshaler, payload)
	}()

	return nil
}

func (h *OpenobserveHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &OpenobserveHandler{
		option: h.option,
		attrs:  slogcommon.AppendAttrsToGroup(h.groups, h.attrs, attrs...),
		groups: h.groups,
	}
}

func (h *OpenobserveHandler) WithGroup(name string) slog.Handler {
	return &OpenobserveHandler{
		option: h.option,
		attrs:  h.attrs,
		groups: append(h.groups, name),
	}
}

func send(endpoint, method string, header http.Header, timeout time.Duration, marshaler func(v any) ([]byte, error), payload map[string]any) error {
	var (
		_method = http.MethodPost
		_header = http.Header{
			"Content-Type": []string{"application/json"},
			"user-agent":   []string{name},
		}
		client = http.Client{
			Timeout: time.Duration(10) * time.Second,
		}
	)

	if len(method) > 0 {
		_method = method
	}

	if len(header) > 0 {
		for k, v := range header {
			_header[k] = v
		}
	}

	json, err := marshaler(payload)
	if err != nil {
		return err
	}

	body := bytes.NewBuffer(json)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// @TODO: maintain a pool of tcp connections
	req, err := http.NewRequestWithContext(ctx, _method, endpoint, body)
	if err != nil {
		return err
	}

	req.Header = _header

	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	return nil
}
