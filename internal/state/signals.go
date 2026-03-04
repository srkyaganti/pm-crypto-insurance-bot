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
