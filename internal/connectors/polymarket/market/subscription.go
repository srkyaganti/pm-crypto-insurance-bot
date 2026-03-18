package market

import (
	"sync"
	"time"
)

type SubscriptionManager struct {
	mu     sync.RWMutex
	assets map[string]bool
	dirty  bool // true when assets changed since last ResubscribeAll
	client *WSClient
}

func newSubscriptionManager(client *WSClient) *SubscriptionManager {
	return &SubscriptionManager{
		assets: make(map[string]bool),
		client: client,
	}
}

func (sm *SubscriptionManager) Subscribe(assetID string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.assets[assetID] {
		return nil
	}

	sm.assets[assetID] = true
	sm.dirty = true
	return nil
}

func (sm *SubscriptionManager) Unsubscribe(assetID string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if !sm.assets[assetID] {
		return nil
	}

	delete(sm.assets, assetID)
	return nil
}

func (sm *SubscriptionManager) SubscribeMany(assetIDs []string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for _, assetID := range assetIDs {
		if !sm.assets[assetID] {
			sm.assets[assetID] = true
			sm.dirty = true
		}
	}

	return nil
}

func (sm *SubscriptionManager) UnsubscribeMany(assetIDs []string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for _, assetID := range assetIDs {
		delete(sm.assets, assetID)
	}
	return nil
}

func (sm *SubscriptionManager) GetCurrentSubscriptions() []string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	assetIDs := make([]string, 0, len(sm.assets))
	for id := range sm.assets {
		assetIDs = append(assetIDs, id)
	}
	return assetIDs
}

// IsDirty returns true if assets changed since the last ResubscribeAll.
func (sm *SubscriptionManager) IsDirty() bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.dirty
}

// ResubscribeAll sends the full asset list — used after reconnection.
func (sm *SubscriptionManager) ResubscribeAll() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.dirty = false

	assetIDs := make([]string, 0, len(sm.assets))
	for id := range sm.assets {
		assetIDs = append(assetIDs, id)
	}

	if len(assetIDs) == 0 || sm.client == nil {
		return nil
	}

	return sm.client.sendSubscription(assetIDs)
}

// SendInitial sends the full list once on first connect (before any dynamic adds).
func (sm *SubscriptionManager) SendInitial() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.dirty = false

	assetIDs := make([]string, 0, len(sm.assets))
	for id := range sm.assets {
		assetIDs = append(assetIDs, id)
	}

	if len(assetIDs) == 0 || sm.client == nil {
		return nil
	}

	// Wait briefly for the WS to be fully ready
	time.Sleep(100 * time.Millisecond)

	return sm.client.sendSubscription(assetIDs)
}
