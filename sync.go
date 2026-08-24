package main

import (
	"archive/tar"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func shouldSkip(rel string) bool {
	rel = strings.TrimPrefix(rel, "./")
	if rel == "" || rel == "." {
		return false
	}
	for _, seg := range strings.Split(rel, "/") {
		if strings.HasPrefix(seg, ".") || seg == "target" || seg == "node_modules" {
			return true
		}
	}
	return false
}

func listWorkspaceFiles(root string) []string {
	var out []string
	filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info == nil {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		if shouldSkip(rel) {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if rel == "." {
			return nil
		}
		if !info.IsDir() {
			out = append(out, rel)
		}
		return nil
	})
	return out
}

func extractTarball(r io.Reader, dest string) (int, error) {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return 0, err
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	count := 0
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return count, err
		}
		count++
		target := filepath.Join(dest, hdr.Name)
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return count, err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return count, err
			}
			f, err := os.Create(target)
			if err != nil {
				return count, err
			}
			if _, err := io.Copy(f, tr); err != nil {
				f.Close()
				return count, err
			}
			f.Close()
		}
	}
	return count, nil
}

func createTarball(w io.Writer, base string, entries []string) error {
	gz := gzip.NewWriter(w)
	defer gz.Close()
	tw := tar.NewWriter(gz)
	defer tw.Close()

	seen := map[string]bool{}
	for _, entry := range entries {
		full := filepath.Join(base, entry)
		err := filepath.Walk(full, func(path string, info os.FileInfo, err error) error {
			if err != nil || info == nil {
				return nil
			}
			rel, _ := filepath.Rel(base, path)
			if rel == "." {
				return nil
			}
			if shouldSkip(rel) {
				if info.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
			rel = filepath.ToSlash(rel)
			if seen[rel] {
				return nil
			}
			seen[rel] = true
			hdr, err := tar.FileInfoHeader(info, "")
			if err != nil {
				return err
			}
			hdr.Name = rel
			if err := tw.WriteHeader(hdr); err != nil {
				return err
			}
			if info.Mode().IsRegular() {
				f, err := os.Open(path)
				if err != nil {
					return err
				}
				if _, err := io.Copy(tw, f); err != nil {
					f.Close()
					return err
				}
				f.Close()
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}
