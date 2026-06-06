package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
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
