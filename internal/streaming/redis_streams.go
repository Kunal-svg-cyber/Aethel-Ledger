
package streaming

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/Kunal-svg-cyber/aethel-ledger/internal/ledger"
)

type RedisStreamsBus struct {
	baseURL    string // e.g. https://your-db.upstash.io
	token      string
	streamName string
	client     *http.Client
}

func NewRedisStreamsBus(baseURL, token, streamName string) *RedisStreamsBus {
	return &RedisStreamsBus{
		baseURL:    baseURL,
		token:      token,
		streamName: streamName,
		client:     &http.Client{},
	}
}

// Publish appends ev to the stream via XADD with an auto-generated ID.
func (b *RedisStreamsBus) Publish(ctx context.Context, ev ledger.Event) error {
	cmd := []interface{}{
		"XADD", b.streamName, "*",
		"seq", strconv.FormatInt(ev.Seq, 10),
		"type", string(ev.Type),
		"account", ev.Account,
		"counter_account", ev.CounterAccount,
		"amount", strconv.FormatInt(ev.Amount, 10),
	}
	_, err := b.do(ctx, cmd)
	return err
}

type StreamEntry struct {
	ID     string
	Fields map[string]string
}

func (b *RedisStreamsBus) ReadRange(ctx context.Context, fromIDExclusive string) ([]StreamEntry, error) {
	start := "-"
	if fromIDExclusive != "" {
		start = "(" + fromIDExclusive // "(" = exclusive lower bound, per Redis XRANGE syntax
	}
	cmd := []interface{}{"XRANGE", b.streamName, start, "+"}

	result, err := b.do(ctx, cmd)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, nil
	}

	raw, ok := result.([]interface{})
	if !ok {
		return nil, fmt.Errorf("streaming: unexpected XRANGE result shape: %T", result)
	}

	entries := make([]StreamEntry, 0, len(raw))
	for _, r := range raw {
		pair, ok := r.([]interface{})
		if !ok || len(pair) != 2 {
			continue
		}
		id, _ := pair[0].(string)
		fieldsRaw, ok := pair[1].([]interface{})
		if !ok {
			continue
		}
		fields := make(map[string]string, len(fieldsRaw)/2)
		for i := 0; i+1 < len(fieldsRaw); i += 2 {
			k, _ := fieldsRaw[i].(string)
			v, _ := fieldsRaw[i+1].(string)
			fields[k] = v
		}
		entries = append(entries, StreamEntry{ID: id, Fields: fields})
	}
	return entries, nil
}

type restResponse struct {
	Result interface{} `json:"result"`
	Error  string      `json:"error"`
}

func (b *RedisStreamsBus) do(ctx context.Context, cmd []interface{}) (interface{}, error) {
	body, err := json.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("streaming: marshal command: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.baseURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("streaming: build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+b.token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := b.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("streaming: request failed: %w", err)
	}
	defer resp.Body.Close()

	var out restResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("streaming: decode response: %w", err)
	}
	if out.Error != "" {
		return nil, fmt.Errorf("streaming: upstash error: %s", out.Error)
	}
	return out.Result, nil
}
