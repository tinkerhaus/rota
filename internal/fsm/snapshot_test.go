package fsm

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/hashicorp/raft"

	"github.com/tinkerhaus/rota/internal/storage"
)

// bufSink is a minimal raft.SnapshotSink backed by a buffer.
type bufSink struct{ buf *bytes.Buffer }

func (b *bufSink) Write(p []byte) (int, error) { return b.buf.Write(p) }
func (b *bufSink) Close() error                { return nil }
func (b *bufSink) ID() string                  { return "test" }
func (b *bufSink) Cancel() error               { return nil }

func applyCmd(t *testing.T, f *FSM, idx uint64, c Command) {
	t.Helper()
	data, _ := json.Marshal(c)
	if r := f.Apply(&raft.Log{Index: idx, Type: raft.LogCommand, Data: data}); r != nil {
		if err, ok := r.(error); ok {
			t.Fatalf("apply: %v", err)
		}
	}
}

// A snapshot of the application keyspace round-trips into a fresh store, carrying
// messages, group counters, and the applied index — and excludes the raft log.
func TestSnapshotRestoreRoundTrip(t *testing.T) {
	s1, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s1.Close()
	f1, _ := New(s1)
	for i := uint64(1); i <= 5; i++ {
		applyCmd(t, f1, i, Command{Type: CmdPublish, Publish: &PublishCmd{
			Lane: "l", GroupID: "g", Payload: []byte("x"), NowMs: 1000,
		}})
	}

	snap, err := f1.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&bufSink{buf: &buf}); err != nil {
		t.Fatal(err)
	}
	snap.Release()

	s2, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	f2, _ := New(s2)
	if err := f2.Restore(io.NopCloser(bytes.NewReader(buf.Bytes()))); err != nil {
		t.Fatal(err)
	}

	groups, err := s2.ListGroups("l")
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 1 || groups[0].Ready != 5 {
		t.Fatalf("restored groups = %+v, want 1 group ready=5", groups)
	}
	if f2.appliedIndex != 5 {
		t.Fatalf("restored applied index = %d, want 5", f2.appliedIndex)
	}
}
