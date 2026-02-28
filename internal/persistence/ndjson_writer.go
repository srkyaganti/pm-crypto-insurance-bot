package persistence

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
)

const (
	DefaultChannelSize   = 10000
	DefaultFlushInterval = 1 * time.Second
)

type Writer struct {
	ch            chan *Event
	rotator       *Rotator
	logger        *zerolog.Logger
	flushInterval time.Duration
	wg            sync.WaitGroup

	stats   writerStatsInternal
	stopCh  chan struct{}
	stopMux sync.Mutex
	running atomic.Bool
}

func NewWriter(ndjsonPath string, flushIntervalMs int, logger *zerolog.Logger) (*Writer, error) {
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
		stopCh:        make(chan struct{}),
	}, nil
}

func (w *Writer) Start(ctx context.Context, wg *sync.WaitGroup) error {
	w.running.Store(true)
	wg.Add(1)
	w.wg.Add(1)

	go w.run(ctx, wg)

	w.logger.Info().
		Str("path", w.rotator.CurrentPath()).
		Dur("flush_interval", w.flushInterval).
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
