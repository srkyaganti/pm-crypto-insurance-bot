package state

func (s *BinanceSpotState) BuyImbalance10s() float64 {
	return s.TradeFlow.Window10s.Imbalance
}

func (s *BinanceSpotState) SellImbalance10s() float64 {
	return -s.TradeFlow.Window10s.Imbalance
}

func (s *BinanceSpotState) BuyImbalance30s() float64 {
	return s.TradeFlow.Window30s.Imbalance
}

func (s *BinanceSpotState) LiquidityToPrice(targetPrice float64) float64 {
	mid := s.OrderBook.MidPrice
	if mid == 0 {
		return 0
	}

	var liquidity float64
	if targetPrice > mid {
		for _, ask := range s.OrderBook.Asks {
			if ask.Price <= targetPrice {
				liquidity += ask.Size
			}
		}
	} else {
		for _, bid := range s.OrderBook.Bids {
			if bid.Price >= targetPrice {
				liquidity += bid.Size
			}
		}
	}
	return liquidity
}

// BookImbalanceNearPrice returns bid_volume / (bid_volume + ask_volume) for levels
// within pctRange% of targetPrice. Values > 0.5 mean more bids (bullish), < 0.5 more asks (bearish).
// Returns 0.5 (neutral) if no levels are in range.
func (s *BinanceSpotState) BookImbalanceNearPrice(targetPrice float64, pctRange float64) float64 {
	if targetPrice <= 0 || pctRange <= 0 {
		return 0.5
	}
	low := targetPrice * (1 - pctRange/100)
	high := targetPrice * (1 + pctRange/100)

	var bidVol, askVol float64
	for _, bid := range s.OrderBook.Bids {
		if bid.Price >= low && bid.Price <= high {
			bidVol += bid.Size * bid.Price
		}
	}
	for _, ask := range s.OrderBook.Asks {
		if ask.Price >= low && ask.Price <= high {
			askVol += ask.Size * ask.Price
		}
	}
	total := bidVol + askVol
	if total == 0 {
		return 0.5
	}
	return bidVol / total
}

func (s *BinanceSpotState) FlowMomentum() float64 {
	return (s.TradeFlow.Window10s.Imbalance * 0.7) + (s.TradeFlow.Window30s.Imbalance * 0.3)
}

func (s *BinanceSpotState) IsLiquidationBarrier(price float64, thresholdSize float64) bool {
	liquidity := s.LiquidityToPrice(price)
	return liquidity >= thresholdSize
}

func (s *BinanceSpotState) FlowIntensity10s() float64 {
	return s.TradeFlow.Window10s.BuyNotional + s.TradeFlow.Window10s.SellNotional
}

func (s *BinanceSpotState) FlowIntensity30s() float64 {
	return s.TradeFlow.Window30s.BuyNotional + s.TradeFlow.Window30s.SellNotional
}

func (s *BinanceSpotState) GetSpread() float64 {
	if s.OrderBook.BestAsk > 0 && s.OrderBook.BestBid > 0 {
		return s.OrderBook.BestAsk - s.OrderBook.BestBid
	}
	return 0
}

func (s *BinanceSpotState) GetSpreadBps() float64 {
	spread := s.GetSpread()
	midPrice := s.OrderBook.MidPrice
	if midPrice > 0 {
		return (spread / midPrice) * 10000
	}
	return 0
}
