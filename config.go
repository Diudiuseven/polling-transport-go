package transport

import (
	"fmt"
	"time"
)

// Config controls the transport server behavior.
type Config struct {
	// PollInterval is the suggested minimum interval between poll requests.
	// Must be > 0.
	PollInterval time.Duration
	// LongPollTimeout is the maximum time a long-poll request suspends.
	// Must be > 0.
	LongPollTimeout time.Duration
	// BufferSize is the per-subscriber event buffer capacity for streams.
	// Must be > 0.
	BufferSize int
	// MaxSubscriptions is the maximum number of concurrent subscriptions.
	// Must be > 0.
	MaxSubscriptions int
	// HeartbeatInterval is the idle heartbeat interval for streams.
	// Must be > 0.
	HeartbeatInterval time.Duration
}

// DefaultConfig returns a configuration with sensible defaults.
func DefaultConfig() Config {
	return Config{
		PollInterval:      100 * time.Millisecond,
		LongPollTimeout:   30 * time.Second,
		BufferSize:        64,
		MaxSubscriptions:  1024,
		HeartbeatInterval: 15 * time.Second,
	}
}

// Validate checks the configuration and returns a readable error.
func (c Config) Validate() error {
	if c.PollInterval <= 0 {
		return fmt.Errorf("%w: PollInterval must be > 0, got %s", ErrInvalidConfig, c.PollInterval)
	}
	if c.LongPollTimeout <= 0 {
		return fmt.Errorf("%w: LongPollTimeout must be > 0, got %s", ErrInvalidConfig, c.LongPollTimeout)
	}
	if c.BufferSize <= 0 {
		return fmt.Errorf("%w: BufferSize must be > 0, got %d", ErrInvalidConfig, c.BufferSize)
	}
	if c.MaxSubscriptions <= 0 {
		return fmt.Errorf("%w: MaxSubscriptions must be > 0, got %d", ErrInvalidConfig, c.MaxSubscriptions)
	}
	if c.HeartbeatInterval <= 0 {
		return fmt.Errorf("%w: HeartbeatInterval must be > 0, got %s", ErrInvalidConfig, c.HeartbeatInterval)
	}
	if c.HeartbeatInterval > c.LongPollTimeout {
		return fmt.Errorf("%w: HeartbeatInterval (%s) should not exceed LongPollTimeout (%s)",
			ErrInvalidConfig, c.HeartbeatInterval, c.LongPollTimeout)
	}
	return nil
}
