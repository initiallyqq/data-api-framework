package service

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is a data service's configuration file (configs/<service>.yaml in its
// repository, installed by the deployment). A key it does not know
// fails the load, so a misspelt or retired key is never silently ignored.
type Config struct {
	ListenAddr               string        `yaml:"listenAddr"`
	DatabasePath             string        `yaml:"databasePath"`
	BackupPath               string        `yaml:"backupPath"`
	AdminTokenFile           string        `yaml:"adminTokenFile"`
	RequestTimeout           time.Duration `yaml:"requestTimeout"`
	DolphinDBAddress         string        `yaml:"dolphinDBAddress"`
	DolphinDBCredentialsFile string        `yaml:"dolphinDBCredentialsFile"`
	DolphinDBDatabase        string        `yaml:"dolphinDBDatabase"`
	SyncInterval             time.Duration `yaml:"syncInterval"`
	SyncRetryInterval        time.Duration `yaml:"syncRetryInterval"`
	MaxStaleness             time.Duration `yaml:"maxStaleness"`
}

// LoadConfig reads and validates the configuration file at path;
// defaultListenAddr is the listen address when it names none.
func LoadConfig(path, defaultListenAddr string) (Config, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(content))
	decoder.KnownFields(true)
	var config Config
	if err := decoder.Decode(&config); err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}
	if err := config.Validate(defaultListenAddr); err != nil {
		return Config{}, err
	}
	return config, nil
}

// Validate fills the defaults and refuses a configuration the service cannot
// run with.
func (c *Config) Validate(defaultListenAddr string) error {
	if c.ListenAddr == "" {
		c.ListenAddr = defaultListenAddr
	}
	if strings.TrimSpace(c.DatabasePath) == "" {
		return errors.New("databasePath is required")
	}
	if strings.TrimSpace(c.BackupPath) == "" {
		return errors.New("backupPath is required")
	}
	c.DatabasePath = filepath.Clean(c.DatabasePath)
	c.BackupPath = filepath.Clean(c.BackupPath)
	if c.DatabasePath == c.BackupPath {
		return errors.New("databasePath and backupPath must differ")
	}
	if strings.TrimSpace(c.AdminTokenFile) == "" {
		return errors.New("adminTokenFile is required")
	}
	if c.RequestTimeout <= 0 {
		c.RequestTimeout = 30 * time.Second
	}
	if strings.TrimSpace(c.DolphinDBAddress) == "" {
		return errors.New("dolphinDBAddress is required")
	}
	if strings.TrimSpace(c.DolphinDBCredentialsFile) == "" {
		return errors.New("dolphinDBCredentialsFile is required")
	}
	if strings.TrimSpace(c.DolphinDBDatabase) == "" {
		return errors.New("dolphinDBDatabase is required")
	}
	if c.SyncInterval <= 0 {
		c.SyncInterval = time.Hour
	}
	if c.SyncRetryInterval <= 0 {
		c.SyncRetryInterval = 5 * time.Minute
	}
	if c.MaxStaleness <= 0 {
		c.MaxStaleness = 48 * time.Hour
	}
	return nil
}
