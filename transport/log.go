package transport

import "sync"

// changeLog 存储已归一化的有序变更序列，支持按游标读取。
// 底层变更可能乱序或重复到达，本结构负责归一化：
// 重复到达被丢弃，乱序到达被暂存，只有连续无空洞的前缀对订阅者可见。
// 变更序号从 1 开始单调递增。
type changeLog struct {
	mu        sync.Mutex
	committed []Change          // 连续可见前缀，下标 i 对应序号 i+1
	pending   map[uint64]Change // 暂存的乱序变更（序号 > 已提交游标）
}

func newChangeLog() *changeLog {
	return &changeLog{pending: make(map[uint64]Change)}
}

// append 归一化写入一条变更，返回归一化后的最新可见游标以及本次
// 调用是否推进了可见前缀（用于决定是否唤醒等待者）。
func (l *changeLog) append(c Change) (head Cursor, advanced bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	base := uint64(len(l.committed))
	if c.Seq == 0 || c.Seq <= base {
		// 重复或已提交的变更：归一化丢弃。
		return Cursor(base), false
	}
	if _, dup := l.pending[c.Seq]; dup {
		return Cursor(base), false
	}
	l.pending[c.Seq] = c

	// 推进连续前缀。
	next := base + 1
	for {
		nc, ok := l.pending[next]
		if !ok {
			break
		}
		delete(l.pending, next)
		l.committed = append(l.committed, nc)
		next++
	}
	newBase := uint64(len(l.committed))
	return Cursor(newBase), newBase > base
}

// read 返回游标之后、至多 limit 条的可见变更。limit<=0 表示不限制。
func (l *changeLog) read(after Cursor, limit int) []Change {
	l.mu.Lock()
	defer l.mu.Unlock()

	start := uint64(after)
	if start >= uint64(len(l.committed)) {
		return nil
	}
	end := uint64(len(l.committed))
	if limit > 0 && start+uint64(limit) < end {
		end = start + uint64(limit)
	}
	out := make([]Change, end-start)
	copy(out, l.committed[start:end])
	return out
}

// head 返回当前最新可见游标。
func (l *changeLog) head() Cursor {
	l.mu.Lock()
	defer l.mu.Unlock()
	return Cursor(len(l.committed))
}
