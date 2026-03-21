package logging

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Config struct {
	Enabled bool
	Bucket  string
	Region  string
	Prefix  string
}

// RotatingWriter implements io.Writer with 15-minute file rotation and optional S3 archival.
type RotatingWriter struct {
	mu          sync.Mutex
	dir         string
	currentFile *os.File
	currentSlot int64
	s3cfg       S3Config
	s3Client    *s3.Client
}

func NewRotatingWriter(dir string, s3cfg S3Config) *RotatingWriter {
	return &RotatingWriter{
		dir:         dir,
		currentSlot: -1,
		s3cfg:       s3cfg,
	}
}

func (w *RotatingWriter) Initialize() error {
	if err := os.MkdirAll(w.dir, 0755); err != nil {
		return fmt.Errorf("create log dir: %w", err)
	}
	return w.rotate(time.Now().UTC())
}

// Write implements io.Writer. Called by zerolog on every log line.
func (w *RotatingWriter) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	now := time.Now().UTC()
	slot := slotID(now)
	if slot != w.currentSlot {
		if err := w.rotateUnlocked(now); err != nil {
			return 0, err
		}
	}

	return w.currentFile.Write(p)
}

// Start begins the S3 archive loop. Call this after the logger is created.
func (w *RotatingWriter) Start(ctx context.Context) error {
	if !w.s3cfg.Enabled {
		return nil
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(w.s3cfg.Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			os.Getenv("AWS_ACCESS_KEY_ID"),
			os.Getenv("AWS_SECRET_ACCESS_KEY"),
			"",
		)),
	)
	if err != nil {
		return fmt.Errorf("logging: load AWS config: %w", err)
	}
	w.s3Client = s3.NewFromConfig(cfg)

	go w.archiveLoop(ctx)
	return nil
}

// Stop flushes and closes the current file.
func (w *RotatingWriter) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.currentFile != nil {
		w.currentFile.Sync()
		w.currentFile.Close()
		w.currentFile = nil
	}
}

func slotID(t time.Time) int64 {
	return t.Unix() / (15 * 60)
}

func slotFilename(t time.Time) string {
	slotStart := t.Truncate(15 * time.Minute)
	return fmt.Sprintf("engine-%s.log", slotStart.Format("2006-01-02-15-04"))
}

func (w *RotatingWriter) rotate(now time.Time) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.rotateUnlocked(now)
}

func (w *RotatingWriter) rotateUnlocked(now time.Time) error {
	slot := slotID(now)

	if w.currentFile != nil {
		w.currentFile.Sync()
		w.currentFile.Close()
	}

	fname := slotFilename(now)
	path := filepath.Join(w.dir, fname)

	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open log file %s: %w", path, err)
	}

	w.currentFile = f
	w.currentSlot = slot
	return nil
}

func (w *RotatingWriter) currentFilename() string {
	return slotFilename(time.Now().UTC())
}

// archiveLoop runs every 5 minutes, uploading completed log files to S3.
func (w *RotatingWriter) archiveLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.archiveOnce()
		}
	}
}

func (w *RotatingWriter) archiveOnce() {
	currentFile := w.currentFilename()

	pattern := filepath.Join(w.dir, "engine-*.log")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return
	}

	sort.Strings(matches)

	for _, path := range matches {
		name := filepath.Base(path)

		if name == currentFile {
			continue
		}

		info, err := os.Stat(path)
		if err != nil {
			continue
		}

		if info.Size() == 0 {
			os.Remove(path)
			continue
		}

		s3Key := w.s3cfg.Prefix + name
		if err := w.uploadToS3(path, s3Key); err != nil {
			continue
		}

		os.Remove(path)
	}
}

func (w *RotatingWriter) uploadToS3(localPath, s3Key string) error {
	f, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer f.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	_, err = w.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &w.s3cfg.Bucket,
		Key:    &s3Key,
		Body:   f,
	})
	return err
}
