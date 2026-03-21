package persistence

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/rs/zerolog"
)

const (
	DefaultChannelSize   = 10000
	DefaultFlushInterval = 1 * time.Second
)

// S3Config holds optional S3 archival settings for the persistence writer.
type S3Config struct {
	Enabled bool
	Bucket  string
	Region  string
	Prefix  string
}

type Writer struct {
	ch            chan *Event
	rotator       *Rotator
	logger        *zerolog.Logger
	flushInterval time.Duration
	wg            sync.WaitGroup

	s3cfg    S3Config
	s3Client *s3.Client

	stats   writerStatsInternal
	stopCh  chan struct{}
	stopMux sync.Mutex
	running atomic.Bool
}

func NewWriter(ndjsonPath string, flushIntervalMs int, s3cfg S3Config, logger *zerolog.Logger) (*Writer, error) {
	flushInterval := time.Duration(flushIntervalMs) * time.Millisecond
	if flushInterval <= 0 {
		flushInterval = DefaultFlushInterval
	}

	rotator := NewRotator(ndjsonPath)
	if err := rotator.Initialize(); err != nil {
		return nil, fmt.Errorf("failed to initialize rotator: %w", err)
	}

	return &Writer{
		ch:            make(chan *Event, DefaultChannelSize),
		rotator:       rotator,
		logger:        logger,
		flushInterval: flushInterval,
		s3cfg:         s3cfg,
		stopCh:        make(chan struct{}),
	}, nil
}

func (w *Writer) Start(ctx context.Context, wg *sync.WaitGroup) error {
	w.running.Store(true)
	wg.Add(1)
	w.wg.Add(1)

	go w.run(ctx, wg)

	if w.s3cfg.Enabled {
		cfg, err := awsconfig.LoadDefaultConfig(ctx,
			awsconfig.WithRegion(w.s3cfg.Region),
			awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
				os.Getenv("AWS_ACCESS_KEY_ID"),
				os.Getenv("AWS_SECRET_ACCESS_KEY"),
				"",
			)),
		)
		if err != nil {
			return fmt.Errorf("persistence: load AWS config: %w", err)
		}
		w.s3Client = s3.NewFromConfig(cfg)
		w.logger.Info().
			Str("bucket", w.s3cfg.Bucket).
			Str("region", w.s3cfg.Region).
			Str("prefix", w.s3cfg.Prefix).
			Msg("persistence: s3 archival enabled")
		go w.archiveLoop(ctx)
	}

	w.logger.Info().
		Str("path", w.rotator.CurrentPath()).
		Dur("flush_interval", w.flushInterval).
		Bool("s3_enabled", w.s3cfg.Enabled).
		Msg("persistence: writer started")

	return nil
}

func (w *Writer) run(ctx context.Context, wg *sync.WaitGroup) {
	defer w.wg.Done()
	defer wg.Done()

	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.logger.Info().Msg("persistence: context cancelled, draining remaining events")
			w.drainAndFlush()
			return

		case <-w.stopCh:
			w.logger.Info().Msg("persistence: stop signal received, draining remaining events")
			w.drainAndFlush()
			return

		case event := <-w.ch:
			if err := w.writeEvent(event); err != nil {
				w.logger.Error().Err(err).Msg("persistence: failed to write event")
			}

		case <-ticker.C:
			if err := w.flush(); err != nil {
				w.logger.Error().Err(err).Msg("persistence: failed to flush")
			}
		}
	}
}

func (w *Writer) Write(event *Event) {
	if !w.running.Load() {
		atomic.AddInt64(&w.stats.eventsDropped, 1)
		return
	}

	select {
	case w.ch <- event:
	default:
		atomic.AddInt64(&w.stats.eventsDropped, 1)
		w.logger.Warn().
			Int("channel_len", len(w.ch)).
			Int("channel_cap", cap(w.ch)).
			Msg("persistence: channel full, event dropped")
	}
}

func (w *Writer) WriteNonBlocking(event *Event) bool {
	if !w.running.Load() {
		atomic.AddInt64(&w.stats.eventsDropped, 1)
		return false
	}

	select {
	case w.ch <- event:
		return true
	default:
		atomic.AddInt64(&w.stats.eventsDropped, 1)
		return false
	}
}

func (w *Writer) writeEvent(event *Event) error {
	file, err := w.rotator.GetCurrentFile()
	if err != nil {
		return fmt.Errorf("failed to get current file: %w", err)
	}

	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	n, err := file.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write event: %w", err)
	}

	_, err = file.Write([]byte("\n"))
	if err != nil {
		return fmt.Errorf("failed to write newline: %w", err)
	}

	atomic.AddInt64(&w.stats.eventsWritten, 1)
	atomic.AddInt64(&w.stats.bytesWritten, int64(n+1))

	return nil
}

func (w *Writer) flush() error {
	if err := w.rotator.Sync(); err != nil {
		return err
	}
	atomic.StoreInt64(&w.stats.lastFlushNano, time.Now().UnixNano())
	return nil
}

func (w *Writer) drainAndFlush() {
	time.Sleep(50 * time.Millisecond)

	drainCount := 0
	for {
		select {
		case event := <-w.ch:
			if err := w.writeEvent(event); err != nil {
				w.logger.Error().Err(err).Msg("persistence: failed to write event during drain")
			}
			drainCount++
		default:
			goto done
		}
	}
done:
	w.logger.Info().Int("drained", drainCount).Msg("persistence: drained remaining events")

	if err := w.flush(); err != nil {
		w.logger.Error().Err(err).Msg("persistence: failed to flush during shutdown")
	}

	w.running.Store(false)
}

func (w *Writer) Stop() {
	w.stopMux.Lock()
	select {
	case <-w.stopCh:
		w.stopMux.Unlock()
		return
	default:
		close(w.stopCh)
	}
	w.stopMux.Unlock()

	w.wg.Wait()

	if err := w.rotator.Close(); err != nil {
		w.logger.Error().Err(err).Msg("persistence: failed to close rotator")
	}

	w.logger.Info().Msg("persistence: writer stopped")
}

func (w *Writer) Stats() WriterStats {
	return WriterStats{
		EventsWritten: atomic.LoadInt64(&w.stats.eventsWritten),
		EventsDropped: atomic.LoadInt64(&w.stats.eventsDropped),
		BytesWritten:  atomic.LoadInt64(&w.stats.bytesWritten),
		CurrentFile:   w.rotator.CurrentPath(),
		LastFlushTime: time.Unix(0, atomic.LoadInt64(&w.stats.lastFlushNano)),
		RotationCount: w.rotator.RotationCount(),
	}
}

func (w *Writer) IsRunning() bool {
	return w.running.Load()
}

func (w *Writer) ChannelLen() int {
	return len(w.ch)
}

func (w *Writer) ChannelCap() int {
	return cap(w.ch)
}

// archiveLoop runs every 5 minutes, uploading completed (non-current) NDJSON files to S3.
func (w *Writer) archiveLoop(ctx context.Context) {
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

func (w *Writer) archiveOnce() {
	currentPath := w.rotator.CurrentPath()
	currentFile := filepath.Base(currentPath)
	dir := filepath.Dir(currentPath)

	pattern := filepath.Join(dir, "events-*.ndjson")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		w.logger.Error().Err(err).Msg("persistence s3: glob failed")
		return
	}

	sort.Strings(matches)

	archived := 0
	for _, path := range matches {
		name := filepath.Base(path)

		// Skip the file currently being written.
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
			w.logger.Error().Err(err).Str("file", name).Msg("persistence s3: upload failed")
			continue
		}

		if err := os.Remove(path); err != nil {
			w.logger.Error().Err(err).Str("file", name).Msg("persistence s3: delete after upload failed")
			continue
		}

		archived++
	}

	if archived > 0 {
		w.logger.Info().Int("archived", archived).Msg("persistence s3: archive completed")
	}
}

func (w *Writer) uploadToS3(localPath, s3Key string) error {
	f, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
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
