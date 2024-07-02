package slogopenobserve

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"log/slog"

	slogcommon "github.com/samber/slog-common"
)

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
		err := send(h.option, payload)
		if err != nil {
			log.Println("[ERROR] failed to send message to openobserve:", err)
		}
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

func send(opts Option, payload map[string]any) error {
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

	if opts.Username != "" && opts.Password != "" {
		userPass := opts.Username + ":" + opts.Password
		b64UserPass := base64.StdEncoding.EncodeToString([]byte(userPass))
		_header.Set("Authorization", "Basic "+b64UserPass)
	}

	if len(opts.CustomHeaders) > 0 {
		for k, v := range opts.CustomHeaders {
			_header.Add(k, v)
		}
	}

	bts, err := opts.Marshaler(payload)
	if err != nil {
		return err
	}
	body := bytes.NewBuffer(bts)

	ctx, cancel := context.WithTimeout(context.Background(), opts.Timeout)
	defer cancel()

	endpointUrl := fmt.Sprintf("%s/api/%s/%s/_multi", opts.Endpoint, opts.Organization, opts.Stream)
	// @TODO: maintain a pool of tcp connections
	req, err := http.NewRequestWithContext(ctx, _method, endpointUrl, body)
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
