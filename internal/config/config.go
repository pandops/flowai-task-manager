// Package config loads task-manager's YAML configuration
// (/etc/flowai/config.yaml, agents.md rule 16). No environment variable
// carries configuration.
package config

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/pandops/flowai-task-manager/internal/claimtoken"
	"github.com/pandops/flowai-task-manager/internal/cursor"
)

// DefaultSchema is the default PostgreSQL schema.
const DefaultSchema = "task_manager_schema"

// Config is the complete configuration.
type Config struct {
	HTTP struct {
		Listen string `yaml:"listen"`
	} `yaml:"http"`
	DB struct {
		Host     string `yaml:"host"`
		Port     int    `yaml:"port"`
		Database string `yaml:"database"`
		User     string `yaml:"user"`
		Password string `yaml:"password"`
		Schema   string `yaml:"schema"`
		SSLMode  string `yaml:"sslmode"`
		CAFile   string `yaml:"ca_file"`
	} `yaml:"db"`
	NATS struct {
		URL             string        `yaml:"url"`
		CAFile          string        `yaml:"ca_file"`
		CredentialsFile string        `yaml:"credentials_file"`
		InboxPrefix     string        `yaml:"inbox_prefix"`
		RequestTimeout  time.Duration `yaml:"request_timeout"`
	} `yaml:"nats"`
	ClaimToken struct {
		SigningKey string `yaml:"signing_key"`
	} `yaml:"claim_token"`
	CursorKeys []CursorKey `yaml:"cursor_keys"`
}

// CursorKey is one cursor HMAC key.
type CursorKey struct {
	ID     int    `yaml:"id"`
	KeyHex string `yaml:"key_hex"`
}

var identifier = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// Parse decodes and validates a configuration document.
func Parse(data []byte) (Config, error) {
	var c Config
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		return Config{}, fmt.Errorf("config: %w", err)
	}
	if c.HTTP.Listen == "" {
		c.HTTP.Listen = ":8080"
	}
	if c.DB.Port == 0 {
		c.DB.Port = 5432
	}
	if c.DB.Database == "" {
		c.DB.Database = "flowai"
	}
	if c.DB.Schema == "" {
		c.DB.Schema = DefaultSchema
	}
	if c.DB.SSLMode == "" {
		c.DB.SSLMode = "verify-full"
	}
	if c.NATS.InboxPrefix == "" {
		c.NATS.InboxPrefix = "_INBOX.task-manager"
	}
	if c.NATS.RequestTimeout == 0 {
		c.NATS.RequestTimeout = 5 * time.Second
	}
	var problems []string
	need := func(ok bool, msg string) {
		if !ok {
			problems = append(problems, msg)
		}
	}
	need(c.DB.Host != "", "db.host is required")
	need(c.DB.User != "", "db.user is required")
	need(identifier.MatchString(c.DB.Schema), "db.schema must be a lowercase SQL identifier")
	need(c.DB.SSLMode == "verify-full", "db.sslmode must be verify-full")
	need(c.DB.CAFile != "", "db.ca_file is required")
	need(strings.HasPrefix(c.NATS.URL, "tls://"), "nats.url must be a tls:// URL")
	need(c.NATS.CAFile != "", "nats.ca_file is required")
	need(c.NATS.CredentialsFile != "", "nats.credentials_file is required")
	need(c.NATS.RequestTimeout > 0, "nats.request_timeout must be positive")
	if _, err := claimtoken.NewSigner(c.ClaimToken.SigningKey); err != nil {
		problems = append(problems, "claim_token.signing_key: "+err.Error())
	}
	need(len(c.CursorKeys) > 0, "cursor_keys needs at least one key")
	for i, k := range c.CursorKeys {
		raw, err := hex.DecodeString(k.KeyHex)
		need(err == nil && len(raw) == 32, "cursor_keys["+strconv.Itoa(i)+"].key_hex must be 32 bytes of hex")
	}
	if len(problems) > 0 {
		return Config{}, errors.New("config: " + strings.Join(problems, "; "))
	}
	return c, nil
}

// Load reads and parses path.
func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("config: %w", err)
	}
	return Parse(data)
}

// CursorCodec builds the list-cursor codec from cursor_keys in their
// configured order: the first key signs new cursors, every key verifies.
func (c Config) CursorCodec() (*cursor.Codec, error) {
	keys := make([]cursor.Key, 0, len(c.CursorKeys))
	for _, k := range c.CursorKeys {
		raw, err := hex.DecodeString(k.KeyHex)
		if err != nil {
			return nil, fmt.Errorf("cursor key %d: %w", k.ID, err)
		}
		keys = append(keys, cursor.Key{ID: k.ID, Key: raw})
	}
	return cursor.New(keys)
}

// quote renders a keyword/value connection-string value.
func quote(v string) string {
	if v != "" && !strings.ContainsAny(v, ` '\`) {
		return v
	}
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(v) + "'"
}

// DSN returns the pgx connection string with search_path set to the schema.
func (c Config) DSN() string {
	parts := []string{
		"host=" + quote(c.DB.Host),
		"port=" + strconv.Itoa(c.DB.Port),
		"dbname=" + quote(c.DB.Database),
		"user=" + quote(c.DB.User),
		"password=" + quote(c.DB.Password),
		"sslmode=" + c.DB.SSLMode,
		"sslrootcert=" + quote(c.DB.CAFile),
		"search_path=" + c.DB.Schema,
	}
	return strings.Join(parts, " ")
}
