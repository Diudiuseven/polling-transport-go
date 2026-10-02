package transport

import (
	"errors"
	"testing"
)

func mkChange(seq int64, key string) Change {
	return Change{Seq: seq, Key: key}
}

func TestLog_AppendAndRead(t *testing.T) {
	l := NewChangeLog()
	added, dup := l.Append(mkChange(1, "a"), mkChange(2, "b"), mkChange(3, "c"))
	if added != 3 || dup != 0 {
		t.Fatalf("added=%d dup=%d, want 3/0", added, dup)
	}
	if got := l.Frontier(); got != 3 {
		t.Fatalf("frontier=%d, want 3", got)
	}

	changes, err := l.Read(0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 3 {
		t.Fatalf("got %d changes, want 3", len(changes))
	}
	for i, c := range changes {
		if c.Seq != int64(i+1) {
			t.Fatalf("change %d has seq %d, want %d", i, c.Seq, i+1)
		}
	}
}

func TestLog_DuplicatesIgnored(t *testing.T) {
	l := NewChangeLog()
	added, dup := l.Append(mkChange(1, "a"), mkChange(1, "a"), mkChange(2, "b"), mkChange(2, "b"))
	if added != 2 || dup != 2 {
		t.Fatalf("added=%d dup=%d, want 2/2", added, dup)
	}
	if got := l.Frontier(); got != 2 {
		t.Fatalf("frontier=%d, want 2", got)
	}
}

func TestLog_OutOfOrderNormalized(t *testing.T) {
	l := NewChangeLog()
	// Arrive out of order: 3 first, then 1, then 2.
	l.Append(mkChange(3, "c"))
	if got := l.Frontier(); got != 0 {
		t.Fatalf("frontier after seq 3 = %d, want 0 (gap)", got)
	}
	l.Append(mkChange(1, "a"))
	if got := l.Frontier(); got != 1 {
		t.Fatalf("frontier after seq 1 = %d, want 1", got)
	}
	l.Append(mkChange(2, "b"))
	if got := l.Frontier(); got != 3 {
		t.Fatalf("frontier after seq 2 = %d, want 3", got)
	}

	changes, err := l.Read(0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 3 || changes[0].Seq != 1 || changes[1].Seq != 2 || changes[2].Seq != 3 {
		t.Fatalf("changes not normalized: %+v", changes)
	}
}

func TestLog_ReadLimitAndCursor(t *testing.T) {
	l := NewChangeLog()
	for i := int64(1); i <= 5; i++ {
		l.Append(mkChange(i, "k"))
	}

	changes, err := l.Read(2, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 2 || changes[0].Seq != 3 || changes[1].Seq != 4 {
		t.Fatalf("got %+v, want seq 3,4", changes)
	}

	// Reading past the frontier returns an error.
	if _, err := l.Read(10, 10); !errors.Is(err, ErrCursorAhead) {
		t.Fatalf("err=%v, want ErrCursorAhead", err)
	}
	// Negative cursor rejected.
	if _, err := l.Read(-1, 10); !errors.Is(err, ErrBadCursor) {
		t.Fatalf("err=%v, want ErrBadCursor", err)
	}
}

func TestLog_WaitChanClosedOnAdvance(t *testing.T) {
	l := NewChangeLog()
	ch := l.WaitChan(0)
	select {
	case <-ch:
		t.Fatal("wait channel should be open before changes")
	default:
	}
	l.Append(mkChange(1, "a"))
	select {
	case <-ch:
	default:
		t.Fatal("wait channel should be closed after frontier advance")
	}
	// Already-advanced cursor returns an immediately closed channel.
	ch2 := l.WaitChan(0)
	select {
	case <-ch2:
	default:
		t.Fatal("WaitChan for cursor behind frontier should be closed")
	}
}

func TestLog_GapFillingWakesWaiters(t *testing.T) {
	l := NewChangeLog()
	l.Append(mkChange(2, "b")) // gap: seq 1 missing
	ch := l.WaitChan(0)
	select {
	case <-ch:
		t.Fatal("should not wake while gap remains")
	default:
	}
	l.Append(mkChange(1, "a")) // fills gap
	select {
	case <-ch:
	default:
		t.Fatal("should wake after gap fills")
	}
}
