package logging

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func TestNewHandler_JSONFormatProducesParseableJSON(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(NewHandler("json", &buf))
	logger.Info("hello", "trace_id", "abc123")

	var decoded map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &decoded); err != nil {
		t.Fatalf("output is not valid JSON: %v (output: %s)", err, buf.String())
	}
	if decoded["msg"] != "hello" {
		t.Fatalf("msg = %v, want hello", decoded["msg"])
	}
	if decoded["trace_id"] != "abc123" {
		t.Fatalf("trace_id = %v, want abc123", decoded["trace_id"])
	}
}

func TestNewHandler_TextFormatIsHumanReadableNotJSON(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(NewHandler("text", &buf))
	logger.Info("hello", "trace_id", "abc123")

	out := buf.String()
	if !strings.Contains(out, "hello") {
		t.Fatalf("expected output to contain the message, got: %s", out)
	}
	if !strings.Contains(out, "trace_id=abc123") {
		t.Fatalf("expected key=value formatted field, got: %s", out)
	}
	if strings.HasPrefix(strings.TrimSpace(out), "{") {
		t.Fatalf("text format should not look like JSON, got: %s", out)
	}
}

func TestNewHandler_DefaultsToTextForUnrecognizedFormat(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(NewHandler("", &buf))
	logger.Info("hello")

	out := strings.TrimSpace(buf.String())
	if strings.HasPrefix(out, "{") {
		t.Fatalf("empty LOG_FORMAT should default to text, not JSON; got: %s", out)
	}

	var buf2 bytes.Buffer
	logger2 := slog.New(NewHandler("yaml-or-whatever", &buf2))
	logger2.Info("hello")
	out2 := strings.TrimSpace(buf2.String())
	if strings.HasPrefix(out2, "{") {
		t.Fatalf("unrecognized LOG_FORMAT should default to text, not JSON; got: %s", out2)
	}
}
