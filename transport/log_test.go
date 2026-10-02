package transport

import "testing"

func TestLogAppendInOrder(t *testing.T) {
	l := newChangeLog()
	for i := uint64(1); i <= 5; i++ {
		head, advanced := l.append(Change{Seq: i, Key: "k"})
		if !advanced || head != Cursor(i) {
			t.Fatalf("seq=%d: head=%d advanced=%v", i, head, advanced)
		}
	}
	if got := l.head(); got != 5 {
		t.Fatalf("head=%d, want 5", got)
	}
}

func TestLogAppendOutOfOrder(t *testing.T) {
	l := newChangeLog()
	// 乱序到达：2、3 先到，1 后到。
	if _, adv := l.append(Change{Seq: 2}); adv {
		t.Fatal("seq=2 不应推进可见前缀（存在空洞）")
	}
	if _, adv := l.append(Change{Seq: 3}); adv {
		t.Fatal("seq=3 不应推进可见前缀")
	}
	if got := l.head(); got != 0 {
		t.Fatalf("head=%d, want 0", got)
	}
	head, adv := l.append(Change{Seq: 1})
	if !adv || head != 3 {
		t.Fatalf("补齐空洞后 head=%d advanced=%v, want 3/true", head, adv)
	}
	got := l.read(0, 0)
	if len(got) != 3 || got[0].Seq != 1 || got[2].Seq != 3 {
		t.Fatalf("read=%+v", got)
	}
}

func TestLogAppendDuplicate(t *testing.T) {
	l := newChangeLog()
	l.append(Change{Seq: 1, Value: []byte("a")})
	// 重复到达已提交与暂存的变更都应被丢弃。
	if _, adv := l.append(Change{Seq: 1, Value: []byte("b")}); adv {
		t.Fatal("重复 seq=1 不应推进")
	}
	l.append(Change{Seq: 3})
	if _, adv := l.append(Change{Seq: 3}); adv {
		t.Fatal("重复暂存 seq=3 不应推进")
	}
	l.append(Change{Seq: 2})
	got := l.read(0, 0)
	if len(got) != 3 || string(got[0].Value) != "a" {
		t.Fatalf("read=%+v", got)
	}
}

func TestLogReadByCursor(t *testing.T) {
	l := newChangeLog()
	for i := uint64(1); i <= 10; i++ {
		l.append(Change{Seq: i})
	}
	got := l.read(4, 3)
	if len(got) != 3 || got[0].Seq != 5 || got[2].Seq != 7 {
		t.Fatalf("read(4,3)=%+v", got)
	}
	if got := l.read(10, 0); len(got) != 0 {
		t.Fatalf("read(10) 应为空, got %+v", got)
	}
	if got := l.read(8, 0); len(got) != 2 {
		t.Fatalf("read(8) 应有 2 条, got %d", len(got))
	}
}
