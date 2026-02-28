package lifecycle

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/rs/zerolog"
)

type Supervisor struct {
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	logger  *zerolog.Logger
	running bool
	mu      sync.Mutex
}

func NewSupervisor(logger *zerolog.Logger) *Supervisor {
	ctx, cancel := context.WithCancel(context.Background())
	return &Supervisor{
		ctx:     ctx,
		cancel:  cancel,
		logger:  logger,
		running: false,
	}
}

func (s *Supervisor) Context() context.Context {
	return s.ctx
}

func (s *Supervisor) Cancel() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancel != nil {
		s.cancel()
	}
}

func (s *Supervisor) Add(delta int) {
	s.wg.Add(delta)
}

func (s *Supervisor) Done() {
	s.wg.Done()
}

func (s *Supervisor) Wait() {
	s.wg.Wait()
}

func (s *Supervisor) Shutdown() {
	s.logger.Info().Msg("supervisor: initiating shutdown")
	s.Cancel()
	s.Wait()
	s.logger.Info().Msg("supervisor: shutdown complete")
}

func (s *Supervisor) WaitForSignal() <-chan os.Signal {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	return sigChan
}

func (s *Supervisor) RunWithSignal(handler func() error) error {
	sigChan := s.WaitForSignal()

	errChan := make(chan error, 1)
	go func() {
		errChan <- handler()
	}()

	select {
	case sig := <-sigChan:
		s.logger.Info().Str("signal", sig.String()).Msg("supervisor: received signal, initiating graceful shutdown")
		s.Shutdown()
		return nil
	case err := <-errChan:
		if err != nil {
			s.logger.Error().Err(err).Msg("supervisor: handler returned error")
			s.Shutdown()
		}
		return err
	case <-s.ctx.Done():
		s.logger.Info().Msg("supervisor: context cancelled")
		s.Wait()
		return nil
	}
}

func (s *Supervisor) IsRunning() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}

func (s *Supervisor) SetRunning(running bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.running = running
}

func (s *Supervisor) WaitGroup() *sync.WaitGroup {
	return &s.wg
}
