package main

import (
	"archive/tar"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func cmdBackup(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: rota backup <create|restore> [flags]")
	}
	switch args[0] {
	case "create":
		return cmdBackupCreate(args[1:])
	case "restore":
		return cmdBackupRestore(args[1:])
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
