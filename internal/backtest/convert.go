package backtest

import (
	"time"

	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/connectors/polymarket/tracker"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/recorder"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/state"
)

func convertTicks(ticks []recorder.TickData) state.RingBuffer {
	rb := state.RingBuffer{
		Buffer:   make([]state.PriceTick, 0, 180),
		Capacity: 180,
	}
	for _, t := range ticks {
		rb.Add(state.PriceTick{
			Price:     t.Price,
			Timestamp: time.UnixMilli(t.TS),
		})
	}
	return rb
}

func convertLevels(levels []recorder.LevelData) []state.OrderLevel {
	out := make([]state.OrderLevel, len(levels))
	for i, l := range levels {
		out[i] = state.OrderLevel{Price: l.Price, Size: l.Size}
	}
	return out
}

func convertChainlink(cl recorder.CLData) state.ChainlinkState {
	return state.ChainlinkState{
		LatestPrice: cl.Price,
		IsStale:     cl.Stale,
		Ticks:       convertTicks(cl.Ticks),
	}
}

func convertSpot(sp recorder.SpotData) state.BinanceSpotState {
	return state.BinanceSpotState{
		OrderBook: state.OrderBookSnapshot{
			Bids:     convertLevels(sp.Bids),
			Asks:     convertLevels(sp.Asks),
			BestBid:  sp.BestBid,
			BestAsk:  sp.BestAsk,
			MidPrice: sp.Mid,
		},
		IsStale: sp.Stale,
	}
}

func convertPerps(pp recorder.PerpsData) state.BinancePerpsState {
	return state.BinancePerpsState{
		LastMarkPrice:   pp.MarkPrice,
		LiquidationFlag: pp.LiquidationFlag,
		IsStale:         pp.Stale,
	}
}

func convertMarket(m recorder.MarketData) tracker.ActiveWindowSummary {
	return tracker.ActiveWindowSummary{
		Slug:           m.Slug,
		Symbol:         m.Symbol,
		StartPrice:     m.StartPrice,
		TimeRemainingS: m.TimeRemainingS,
		YesBid:         m.YesBid,
		YesAsk:         m.YesAsk,
		NoBid:          m.NoBid,
		NoAsk:          m.NoAsk,
		HasPosition:    false,
	}
}
