package slogopenobserve

import (
	"log/slog"
	"maps"

	slogcommon "github.com/samber/slog-common"
)

var SourceKey = "source"
var ErrorKeys = []string{"error", "err"}

type Converter func(addSource bool, replaceAttr func(groups []string, a slog.Attr) slog.Attr, loggerAttr []slog.Attr, groups []string, record *slog.Record) map[string]any

func DefaultConverter(addSource bool, replaceAttr func(groups []string, a slog.Attr) slog.Attr, loggerAttr []slog.Attr, groups []string, record *slog.Record) map[string]any {
	// aggregate all attributes
	attrs := slogcommon.AppendRecordAttrsToAttrs(loggerAttr, groups, record)

	// developer formatters
	if addSource {
		attrs = append(attrs, slogcommon.Source(SourceKey, record))
	}
	attrs = slogcommon.ReplaceAttrs(replaceAttr, []string{}, attrs...)
	attrs = slogcommon.RemoveEmptyAttrs(attrs)

	// handler formatter
	log := mapPool.Get().(map[string]any)
	log["@timestamp"] = record.Time.UTC()
	log["logger.name"] = name
	log["logger.version"] = version
	log["level"] = record.Level.String()
	log["message"] = record.Message

	extra := slogcommon.AttrsToMap(attrs...)

	for _, errorKey := range ErrorKeys {
		if err, ok := extra[errorKey]; ok {
			if e, ok := err.(error); ok {
				log[errorKey] = slogcommon.FormatError(e)
				delete(extra, errorKey)
				break
			}
		}
	}

	maps.Copy(log, extra)
	return log
}
