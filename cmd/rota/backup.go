package main

import (
	"archive/tar"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/pebble"

	"github.com/tinkerhaus/rota/internal/storage"
)

func cmdBackup(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: rota backup <create|restore|validate> [flags]")
	}
	switch args[0] {
	case "create":
		return cmdBackupCreate(args[1:])
	case "restore":
		return cmdBackupRestore(args[1:])
	case "validate":
		return cmdBackupValidate(args[1:])
	default:
		return fmt.Errorf("unknown backup command %q", args[0])
	}
}

func cmdBackupCreate(args []string) error {
	var dataDir, outPath string
	fs := flag.NewFlagSet("backup create", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.StringVar(&dataDir, "data", "./data", "stopped Rota data directory to archive")
	fs.StringVar(&outPath, "out", "", "output .tar.gz path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if outPath == "" {
		return fmt.Errorf("--out is required")
	}
	if err := createBackupArchive(dataDir, outPath); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "backup created data=%s out=%s\n", dataDir, outPath)
	return nil
}

func cmdBackupRestore(args []string) error {
	var inPath, dataDir string
	var force bool
	fs := flag.NewFlagSet("backup restore", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.StringVar(&inPath, "in", "", "input .tar.gz backup")
	fs.StringVar(&dataDir, "data", "./data", "target data directory")
	fs.BoolVar(&force, "force", false, "allow restoring into a non-empty directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if inPath == "" {
		return fmt.Errorf("--in is required")
	}
	if err := restoreBackupArchive(inPath, dataDir, force); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "backup restored in=%s data=%s\n", inPath, dataDir)
	return nil
}

type backupValidationReport struct {
	Archive        string `json:"archive"`
	RestoreDir     string `json:"restore_dir"`
	Temporary      bool   `json:"temporary"`
	OK             bool   `json:"ok"`
	Files          int    `json:"files"`
	AppliedIndex   uint64 `json:"applied_index"`
	Messages       int    `json:"messages"`
	Groups         int    `json:"groups"`
	Leases         int    `json:"leases"`
	DeadLetters    int    `json:"dead_letters"`
	Timers         int    `json:"timers"`
	WorkflowRuns   int    `json:"workflow_runs"`
	AuthPrincipals int    `json:"auth_principals"`
}

type quietPebbleLogger struct{}

func (quietPebbleLogger) Infof(string, ...interface{}) {}

func (quietPebbleLogger) Fatalf(format string, args ...interface{}) {
	panic(fmt.Sprintf(format, args...))
}

func cmdBackupValidate(args []string) error {
	var inPath, restoreDir string
	var force, asJSON bool
	fs := flag.NewFlagSet("backup validate", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.StringVar(&inPath, "in", "", "input .tar.gz backup")
	fs.StringVar(&restoreDir, "restore-dir", "", "directory to restore into for validation; omitted uses a temporary directory")
	fs.BoolVar(&force, "force", false, "allow restoring into a non-empty --restore-dir")
	fs.BoolVar(&asJSON, "json", false, "emit JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if inPath == "" {
		return fmt.Errorf("--in is required")
	}
	report, cleanup, err := validateBackupArchive(inPath, restoreDir, force)
	if cleanup != nil {
		defer cleanup()
	}
	if err != nil {
		return err
	}
	return writeBackupValidationReport(os.Stdout, report, asJSON)
}

func validateBackupArchive(inPath, restoreDir string, force bool) (*backupValidationReport, func(), error) {
	temp := false
	cleanup := func() {}
	if restoreDir == "" {
		dir, err := os.MkdirTemp("", "rota-backup-validate-*")
		if err != nil {
			return nil, nil, err
		}
		restoreDir = dir
		temp = true
		cleanup = func() { _ = os.RemoveAll(dir) }
	}
	if err := restoreBackupArchive(inPath, restoreDir, force); err != nil {
		cleanup()
		return nil, nil, err
	}
	report := &backupValidationReport{Archive: inPath, RestoreDir: restoreDir, Temporary: temp}
	files, err := countRegularFiles(restoreDir)
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	report.Files = files
	st, err := storage.OpenWithOptions(restoreDir, &pebble.Options{Logger: quietPebbleLogger{}})
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	failAfterOpen := func(err error) (*backupValidationReport, func(), error) {
		_ = st.Close()
		cleanup()
		return nil, nil, err
	}
	if raw, ok, err := st.GetRaw(storage.MetaKey("applied_index")); err != nil {
		return failAfterOpen(err)
	} else if ok && len(raw) >= 8 {
		report.AppliedIndex = binary.BigEndian.Uint64(raw[:8])
	}
	lo, hi := storage.MessageBounds()
	if report.Messages, err = countRange(st.DB, lo, hi); err != nil {
		return failAfterOpen(err)
	}
	lo, hi = storage.GroupMetaBounds()
	if report.Groups, err = countRange(st.DB, lo, hi); err != nil {
		return failAfterOpen(err)
	}
	lo, hi = storage.LeaseBounds()
	if report.Leases, err = countRange(st.DB, lo, hi); err != nil {
		return failAfterOpen(err)
	}
	lo, hi = storage.DLQBounds()
	if report.DeadLetters, err = countRange(st.DB, lo, hi); err != nil {
		return failAfterOpen(err)
	}
	lo, hi = storage.TimeIndexBounds()
	if report.Timers, err = countRange(st.DB, lo, hi); err != nil {
		return failAfterOpen(err)
	}
	lo, hi = storage.WFRunBounds()
	if report.WorkflowRuns, err = countRange(st.DB, lo, hi); err != nil {
		return failAfterOpen(err)
	}
	lo, hi = storage.AuthPrincipalBounds()
	if report.AuthPrincipals, err = countRange(st.DB, lo, hi); err != nil {
		return failAfterOpen(err)
	}
	if err := st.Close(); err != nil {
		cleanup()
		return nil, nil, err
	}
	report.OK = true
	return report, cleanup, nil
}

func createBackupArchive(dataDir, outPath string) error {
	info, err := os.Stat(dataDir)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", dataDir)
	}
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		return err
	}
	out, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer out.Close()
	gz := gzip.NewWriter(out)
	defer gz.Close()
	tw := tar.NewWriter(gz)
	defer tw.Close()

	root, err := filepath.Abs(dataDir)
	if err != nil {
		return err
	}
	return filepath.WalkDir(root, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == root {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() && !info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		hdr, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		hdr.Name = rel
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()
		_, err = io.Copy(tw, in)
		return err
	})
}

func countRegularFiles(root string) (int, error) {
	n := 0
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == root || d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			n++
		}
		return nil
	})
	return n, err
}

func countRange(db *pebble.DB, lo, hi []byte) (int, error) {
	it, err := db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return 0, err
	}
	defer it.Close()
	n := 0
	for it.First(); it.Valid(); it.Next() {
		n++
	}
	return n, nil
}

func writeBackupValidationReport(w io.Writer, report *backupValidationReport, asJSON bool) error {
	if asJSON {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(report)
	}
	fmt.Fprintf(w, "backup validation ok=%t archive=%s restore_dir=%s\n", report.OK, report.Archive, report.RestoreDir)
	fmt.Fprintf(w, "files=%d applied_index=%d\n", report.Files, report.AppliedIndex)
	fmt.Fprintf(w, "keys: messages=%d groups=%d leases=%d dlq=%d timers=%d workflows=%d auth_principals=%d\n",
		report.Messages, report.Groups, report.Leases, report.DeadLetters, report.Timers, report.WorkflowRuns, report.AuthPrincipals)
	return nil
}

func restoreBackupArchive(inPath, dataDir string, force bool) error {
	if !force {
		empty, err := dirEmptyOrMissing(dataDir)
		if err != nil {
			return err
		}
		if !empty {
			return fmt.Errorf("target data directory %s is not empty; use --force to overwrite", dataDir)
		}
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return err
	}
	in, err := os.Open(inPath)
	if err != nil {
		return err
	}
	defer in.Close()
	gz, err := gzip.NewReader(in)
	if err != nil {
		return err
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	root, err := filepath.Abs(dataDir)
	if err != nil {
		return err
	}
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		target, err := safeRestorePath(root, hdr.Name)
		if err != nil {
			return err
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, os.FileMode(hdr.Mode)); err != nil {
				return err
			}
		case tar.TypeReg, tar.TypeRegA:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(hdr.Mode))
			if err != nil {
				return err
			}
			if _, err := io.Copy(out, tr); err != nil {
				_ = out.Close()
				return err
			}
			if err := out.Close(); err != nil {
				return err
			}
		}
	}
}

func dirEmptyOrMissing(path string) (bool, error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	defer f.Close()
	_, err = f.Readdirnames(1)
	if err == io.EOF {
		return true, nil
	}
	return false, err
}

func safeRestorePath(root, name string) (string, error) {
	if name == "" || filepath.IsAbs(name) || strings.Contains(name, "\x00") {
		return "", fmt.Errorf("unsafe backup path %q", name)
	}
	clean := filepath.Clean(filepath.FromSlash(name))
	if clean == "." || strings.HasPrefix(clean, ".."+string(os.PathSeparator)) || clean == ".." {
		return "", fmt.Errorf("unsafe backup path %q", name)
	}
	target := filepath.Join(root, clean)
	if target != root && !strings.HasPrefix(target, root+string(os.PathSeparator)) {
		return "", fmt.Errorf("unsafe backup path %q", name)
	}
	return target, nil
}
