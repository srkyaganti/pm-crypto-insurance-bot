package market

import (
	"sync"
)

type SubscriptionManager struct {
	mu     sync.RWMutex
	assets map[string]bool
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
	return sm.sendSubscription()
}

func (sm *SubscriptionManager) Unsubscribe(assetID string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if !sm.assets[assetID] {
		return nil
	}

	delete(sm.assets, assetID)
	return sm.sendSubscription()
}

func (sm *SubscriptionManager) SubscribeMany(assetIDs []string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for _, assetID := range assetIDs {
		sm.assets[assetID] = true
	}
	return sm.sendSubscription()
}

func (sm *SubscriptionManager) UnsubscribeMany(assetIDs []string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for _, assetID := range assetIDs {
		delete(sm.assets, assetID)
	}
	return sm.sendSubscription()
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

func (sm *SubscriptionManager) sendSubscription() error {
	assetIDs := make([]string, 0, len(sm.assets))
	for id := range sm.assets {
		assetIDs = append(assetIDs, id)
	}

	if sm.client == nil {
		return nil
	}

	return sm.client.sendSubscription(assetIDs)
}
