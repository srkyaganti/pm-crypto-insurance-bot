package execution

import (
	"sync"

	"github.com/rs/zerolog"
)

// BalanceTracker maintains an in-memory USDC balance that is seeded on startup,
// debited on order fill, credited on redeem, and periodically reconciled via GET /balance.
type BalanceTracker struct {
	mu      sync.Mutex
	balance float64
	logger  *zerolog.Logger
}

func NewBalanceTracker(logger *zerolog.Logger) *BalanceTracker {
	return &BalanceTracker{logger: logger}
}

// Seed sets the initial balance from the execution engine.
func (b *BalanceTracker) Seed(amount float64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.balance = amount
	b.logger.Info().Float64("balance", amount).Msg("balance: seeded")
}

// Debit subtracts amount from balance. Returns false if insufficient funds.
func (b *BalanceTracker) Debit(amount float64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.balance < amount {
		b.logger.Warn().
			Float64("requested", amount).
			Float64("available", b.balance).
			Msg("balance: insufficient funds")
		return false
	}
	b.balance -= amount
	b.logger.Debug().Float64("debited", amount).Float64("remaining", b.balance).Msg("balance: debited")
	return true
}

// Credit adds amount to balance (e.g. after redeem payout).
func (b *BalanceTracker) Credit(amount float64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.balance += amount
	b.logger.Debug().Float64("credited", amount).Float64("balance", b.balance).Msg("balance: credited")
}

// Reconcile overwrites in-memory balance with the actual on-chain balance.
func (b *BalanceTracker) Reconcile(actual float64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	prev := b.balance
	b.balance = actual
	b.logger.Info().
		Float64("previous", prev).
		Float64("actual", actual).
		Float64("drift", actual-prev).
		Msg("balance: reconciled")
}

// Get returns the current in-memory balance.
func (b *BalanceTracker) Get() float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.balance
}
