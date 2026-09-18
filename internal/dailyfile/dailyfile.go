// Package dailyfile holds the shared daily-file hygiene helpers used by the
// file-backed features (request log, client stats): gzip compression of a
// finished day's file, age-based retention and crash-leftover recovery.
package dailyfile

import (
	"compress/gzip"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const dayFormat = "2006-01-02"

// Compress gzips src (BestSpeed: files can be large and this runs off the
// hot path) and removes the plain file only on success. Every path closes
// both handles explicitly, and the plain file's handle is closed BEFORE
// the removal: on Windows a delete fails while any handle is open
// (os.Open does not request FILE_SHARE_DELETE).
func Compress(src string, logger *slog.Logger) {
	in, err := os.Open(src)
	if err != nil {
		return
	}
	ok := false
	defer func() {
		_ = in.Close()
		if ok {
			if err := os.Remove(src); err != nil && logger != nil {
				logger.Warn("daily file cleanup failed", "file", src, "err", err)
			}
		}
	}()
	out, err := os.Create(src + ".gz")
	if err != nil {
		if logger != nil {
			logger.Warn("daily file compression failed", "file", src, "err", err)
		}
		return
	}
	gz, err := gzip.NewWriterLevel(out, gzip.BestSpeed)
	if err != nil { // unreachable with a valid level
		_ = out.Close()
		return
	}
	if _, err := io.Copy(gz, in); err != nil {
		if logger != nil {
			logger.Warn("daily file compression failed", "file", src, "err", err)
		}
		_ = gz.Close()
		_ = out.Close()
		return
	}
	if err := gz.Close(); err != nil {
		if logger != nil {
			logger.Warn("daily file compression failed", "file", src, "err", err)
		}
		_ = out.Close()
		return
	}
	if err := out.Close(); err != nil {
		if logger != nil {
			logger.Warn("daily file compression failed", "file", src, "err", err)
		}
		return
	}
	ok = true
}

// Prune deletes files named <prefix><day><suffix> (and their .gz twins)
// whose day is older than keepDays. A file per day turns retention into
// plain deletion: no merging, no rewriting.
func Prune(dir, prefix, suffix, today string, keepDays int, logger *slog.Logger) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	cutoff, err := time.ParseInLocation(dayFormat, today, time.Local)
	if err != nil {
		return
	}
	cutoff = cutoff.AddDate(0, 0, -keepDays)
	for _, de := range entries {
		name := de.Name()
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		day := strings.TrimPrefix(name, prefix)
		day = strings.TrimSuffix(day, ".gz")
		day = strings.TrimSuffix(day, suffix)
		t, err := time.ParseInLocation(dayFormat, day, time.Local)
		if err != nil {
			continue
		}
		if t.Before(cutoff) {
			if err := os.Remove(filepath.Join(dir, name)); err != nil && logger != nil {
				logger.Warn("daily file prune failed", "file", name, "err", err)
			}
		}
	}
}

// SweepStale compresses plain files (<prefix><day><suffix>) left behind by
// a crash mid-rotation: anything not from today is no longer being written.
func SweepStale(dir, prefix, suffix, today string, logger *slog.Logger) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, de := range entries {
		name := de.Name()
		if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
			continue
		}
		day := strings.TrimSuffix(strings.TrimPrefix(name, prefix), suffix)
		if _, err := time.ParseInLocation(dayFormat, day, time.Local); err != nil || day == today {
			continue
		}
		go Compress(filepath.Join(dir, name), logger)
	}
}
