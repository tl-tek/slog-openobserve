package slogopenobserve

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/samber/lo"
	slogcommon "github.com/samber/slog-common"
	"github.com/valyala/fasthttp"
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
	channel       chan map[string]any

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

	sendCh := make(chan map[string]any, 100000)
	o.channel = sendCh
	go async(o) // sync logs to openobserve

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
	select {
	case h.option.channel <- payload:
	default:
	}
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

func async(opts Option) {
	for {
		// read 1k items
		// wait up to 1 second
		items, length, _, ok := lo.BufferWithTimeout(opts.channel, 1000, 1*time.Second)
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
		if opts.Username != "" && opts.Password != "" {
			userPass := opts.Username + ":" + opts.Password
			b64UserPass := base64.StdEncoding.EncodeToString([]byte(userPass))
			req.Header.Set("Authorization", "Basic "+b64UserPass)
		}
		if len(opts.CustomHeaders) > 0 {
			for k, v := range opts.CustomHeaders {
				req.Header.Set(k, v)
			}
		}
		bts, _ := opts.Marshaler(items)
		req.SetBody(bts)
		req.Header.SetMethod(http.MethodPost)
		endpointUrl := fmt.Sprintf("%s/api/%s/%s/_json", opts.Endpoint, opts.Organization, opts.Stream)
		req.SetRequestURI(endpointUrl)
		res := fasthttp.AcquireResponse()
		if err := fasthttp.DoTimeout(req, res, opts.Timeout); err != nil {
			slog.Error("failed to send message to openobserve:", slog.Any("error", err))
		}

		fasthttp.ReleaseRequest(req)
		fasthttp.ReleaseResponse(res)
	}
}
