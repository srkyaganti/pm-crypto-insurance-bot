package persistence

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Rotator struct {
	basePath      string
	currentFile   *os.File
	currentPath   string
	currentSlot   int64
	mu            sync.Mutex
	rotationCount int64
}

func NewRotator(basePath string) *Rotator {
	return &Rotator{
		basePath:    basePath,
		currentSlot: -1,
	}
}

func (r *Rotator) Initialize() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := os.MkdirAll(r.basePath, 0755); err != nil {
		return fmt.Errorf("failed to create log directory: %w", err)
	}

	return r.rotateUnlocked(time.Now().UTC())
}

func (r *Rotator) GetCurrentFile() (*os.File, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	slot := slotID(time.Now().UTC())
	if r.currentFile == nil || slot != r.currentSlot {
		if err := r.rotateUnlocked(time.Now().UTC()); err != nil {
			return nil, err
		}
	}

	return r.currentFile, nil
}

func slotID(t time.Time) int64 {
	return t.Unix() / (15 * 60)
}

func (r *Rotator) generateFilePath(t time.Time) string {
	slotStart := t.Truncate(15 * time.Minute)
	filename := fmt.Sprintf("events-%s.ndjson", slotStart.Format("2006-01-02-15-04"))
	return filepath.Join(r.basePath, filename)
}

func (r *Rotator) rotateUnlocked(now time.Time) error {
	newPath := r.generateFilePath(now)

	if r.currentPath == newPath && r.currentFile != nil {
		return nil
	}

	if r.currentFile != nil {
		if err := r.currentFile.Sync(); err != nil {
			fmt.Fprintf(os.Stderr, "rotator: failed to sync old file: %v\n", err)
		}
		r.currentFile.Close()
	}

	file, err := os.OpenFile(newPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to create log file %s: %w", newPath, err)
	}

	r.currentFile = file
	r.currentPath = newPath
	r.currentSlot = slotID(now)
	r.rotationCount++

	return nil
}

func (r *Rotator) Sync() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.currentFile == nil {
		return nil
	}
	return r.currentFile.Sync()
}

func (r *Rotator) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.currentFile == nil {
		return nil
	}

	if err := r.currentFile.Sync(); err != nil {
		fmt.Fprintf(os.Stderr, "rotator: failed to sync on close: %v\n", err)
	}

	err := r.currentFile.Close()
	r.currentFile = nil
	return err
}

func (r *Rotator) CurrentPath() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentPath
}

func (r *Rotator) RotationCount() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rotationCount
}

func (r *Rotator) ForceRotate() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rotateUnlocked(time.Now().UTC())
}
