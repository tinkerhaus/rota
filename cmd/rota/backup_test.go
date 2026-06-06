package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tinkerhaus/rota/internal/node"
)

func TestBackupCreateAndRestore(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "data")
	if err := os.MkdirAll(filepath.Join(src, "raft"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "CURRENT"), []byte("manifest"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "raft", "log"), []byte("entries"), 0o600); err != nil {
		t.Fatal(err)
	}

	archive := filepath.Join(root, "backup.tar.gz")
	if err := createBackupArchive(src, archive); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(root, "restore")
	if err := restoreBackupArchive(archive, dst, false); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(dst, "raft", "log"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "entries" {
		t.Fatalf("restored raft/log = %q", got)
	}
}

func TestBackupRestoreRefusesNonEmptyTarget(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "data")
	dst := filepath.Join(root, "restore")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "CURRENT"), []byte("manifest"), 0o644); err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(root, "backup.tar.gz")
	if err := createBackupArchive(src, archive); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dst, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dst, "existing"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	err := restoreBackupArchive(archive, dst, false)
	if err == nil || !strings.Contains(err.Error(), "not empty") {
		t.Fatalf("restore err = %v, want not empty refusal", err)
	}
	if err := restoreBackupArchive(archive, dst, true); err != nil {
		t.Fatal(err)
	}
}

func TestBackupValidateRestoredNodeState(t *testing.T) {
	root := t.TempDir()
	data := filepath.Join(root, "node")
	n, err := node.Open(node.Config{DataDir: data, NodeID: "backup-validate"})
	if err != nil {
		t.Fatal(err)
	}
	if err := n.WaitLeader(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	if _, _, err := n.CreateAuthPrincipal("dashboard", []string{"dashboard"}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Publish(node.PublishReq{Lane: "backup", GroupID: "g", Payload: []byte("work")}); err != nil {
		t.Fatal(err)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}

	archive := filepath.Join(root, "backup.tar.gz")
	if err := createBackupArchive(data, archive); err != nil {
		t.Fatal(err)
	}
	report, cleanup, err := validateBackupArchive(archive, "", false)
	if cleanup != nil {
		defer cleanup()
	}
	if err != nil {
		t.Fatal(err)
	}
	if !report.OK || report.Messages == 0 || report.Groups == 0 || report.AuthPrincipals != 1 {
		t.Fatalf("validation report = %+v", report)
	}
}
