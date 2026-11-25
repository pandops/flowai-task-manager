package test

import (
	"encoding/json"
	"os"
	"testing"
)

type consumerDecl struct {
	Durable       string   `json:"durable"`
	Stream        string   `json:"stream"`
	Filters       []string `json:"filter_subjects"`
	AckPolicy     string   `json:"ack_policy"`
	MaxAckPending int      `json:"max_ack_pending"`
}

func loadConsumers(t *testing.T) []consumerDecl {
	t.Helper()
	raw, err := os.ReadFile("../schemas/consumers.json")
	if err != nil {
		t.Fatalf("consumers.json: %v", err)
	}
	var out []consumerDecl
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("consumers.json: %v", err)
	}
	return out
}
