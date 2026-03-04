package market

import (
	"testing"
)

func TestSubscriptionManager_Subscribe(t *testing.T) {
	sm := newSubscriptionManager(nil)

	err := sm.Subscribe("asset1")
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	subs := sm.GetCurrentSubscriptions()
	if len(subs) != 1 {
		t.Errorf("Expected 1 subscription, got %d", len(subs))
	}

	if subs[0] != "asset1" {
		t.Errorf("Expected 'asset1', got '%s'", subs[0])
	}
}

func TestSubscriptionManager_SubscribeDuplicate(t *testing.T) {
	sm := newSubscriptionManager(nil)

	sm.Subscribe("asset1")
	sm.Subscribe("asset1")

	subs := sm.GetCurrentSubscriptions()
	if len(subs) != 1 {
		t.Errorf("Expected 1 subscription after duplicate subscribe, got %d", len(subs))
	}
}

func TestSubscriptionManager_Unsubscribe(t *testing.T) {
	sm := newSubscriptionManager(nil)

	sm.Subscribe("asset1")
	sm.Subscribe("asset2")

	err := sm.Unsubscribe("asset1")
	if err != nil {
		t.Fatalf("Unsubscribe failed: %v", err)
	}

	subs := sm.GetCurrentSubscriptions()
	if len(subs) != 1 {
		t.Errorf("Expected 1 subscription after unsubscribe, got %d", len(subs))
	}

	if subs[0] != "asset2" {
		t.Errorf("Expected 'asset2', got '%s'", subs[0])
	}
}

func TestSubscriptionManager_SubscribeMany(t *testing.T) {
	sm := newSubscriptionManager(nil)

	assetIDs := []string{"asset1", "asset2", "asset3"}
	err := sm.SubscribeMany(assetIDs)
	if err != nil {
		t.Fatalf("SubscribeMany failed: %v", err)
	}

	subs := sm.GetCurrentSubscriptions()
	if len(subs) != 3 {
		t.Errorf("Expected 3 subscriptions, got %d", len(subs))
	}
}

func TestSubscriptionManager_UnsubscribeMany(t *testing.T) {
	sm := newSubscriptionManager(nil)

	sm.SubscribeMany([]string{"asset1", "asset2", "asset3", "asset4"})
	sm.UnsubscribeMany([]string{"asset1", "asset3"})

	subs := sm.GetCurrentSubscriptions()
	if len(subs) != 2 {
		t.Errorf("Expected 2 subscriptions after unsubscribe many, got %d", len(subs))
	}
}
