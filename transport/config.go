package transport

import (
	"fmt"
	"time"
)

// Config 控制传输层行为。零值不合法，需使用 DefaultConfig 或显式填写并校验。
type Config struct {
	PollInterval      time.Duration // 轮询建议间隔（返回给客户端的提示）
	LongPollTimeout   time.Duration // 长轮询单次挂起上限
	MaxBufferSize     int           // 单订阅者缓冲上限（条）
	MaxSubscribers    int           // 最大并发订阅数
	HeartbeatInterval time.Duration // 事件流心跳间隔
}

// DefaultConfig 返回一组合理默认值。
func DefaultConfig() Config {
	return Config{
		PollInterval:      time.Second,
		LongPollTimeout:   30 * time.Second,
		MaxBufferSize:     1024,
		MaxSubscribers:    1024,
		HeartbeatInterval: 15 * time.Second,
	}
}

// Validate 校验配置，非法时返回可读错误。
func (c Config) Validate() error {
	if c.PollInterval <= 0 {
		return fmt.Errorf("transport: PollInterval 必须为正数，当前为 %v", c.PollInterval)
	}
	if c.LongPollTimeout <= 0 {
		return fmt.Errorf("transport: LongPollTimeout 必须为正数，当前为 %v", c.LongPollTimeout)
	}
	if c.MaxBufferSize <= 0 {
		return fmt.Errorf("transport: MaxBufferSize 必须为正数，当前为 %d", c.MaxBufferSize)
	}
	if c.MaxSubscribers <= 0 {
		return fmt.Errorf("transport: MaxSubscribers 必须为正数，当前为 %d", c.MaxSubscribers)
	}
	if c.HeartbeatInterval <= 0 {
		return fmt.Errorf("transport: HeartbeatInterval 必须为正数，当前为 %v", c.HeartbeatInterval)
	}
	if c.HeartbeatInterval >= c.LongPollTimeout {
		return fmt.Errorf("transport: HeartbeatInterval(%v) 必须小于 LongPollTimeout(%v)",
			c.HeartbeatInterval, c.LongPollTimeout)
	}
	return nil
}
