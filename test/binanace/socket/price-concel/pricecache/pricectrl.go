package pricecache

import (
	"context"
	"log"
	"sync"
	"time"
)



type FuturePriceStatus struct { 
	apiKey string 
	Symbols []string
	store *Store
	isFutures bool
	hub *Hub
	listenKey string
	mu sync.RWMutex
	ctx context.Context
	cancel context.CancelFunc
}


func (ty *FuturePriceStatus) Init(apikey string) {

	ty.store = NewStore()
	ty.isFutures = true // 선물로 바꾸려면 true
	ty.apiKey = apikey
	if ty.apiKey == "" {
		log.Fatal("BINANCE_API_KEY 가 필요합니다")
	}

	// listenKey 발급
	var err error
	ty.listenKey, err = getListenKey(ty.apiKey)
	if err != nil {
		log.Fatal("listenKey 발급 에러:", err)
	}

	ty.hub = NewHub()

	// 샘플: 내부 구독자 하나 붙여서 로그로 확인
	priceCh, cancelPrice := ty.hub.SubscribePrice()
	defer cancelPrice()
	go func() {
		for e := range priceCh {
			// log.Printf("[PRICE] %s bid=%.2f ask=%.2f t=%s", e.Symbol, e.BidPrice, e.AskPrice, e.RecvTime.Format(time.RFC3339))
			ty.store.Set(e.Symbol, Tick{
				Bid: e.BidPrice,
				Ask: e.AskPrice,
				BidQty: e.BidQty,
				AskQty: e.AskQty,
				RecvTime: e.RecvTime,
			})
		}
	}()

	userCh, cancelUser := ty.hub.SubscribeUser()
	defer cancelUser()
	go func() {
		for e := range userCh {
			if e.EventType != "" {
				log.Printf("[USER] %s at %d, sym=%s status=%s exec=%s",
					e.EventType, e.EventTime, e.Order.Symbol, e.Order.Status, e.Order.ExecutionType)
			} else {
				log.Printf("[USER] raw=%s", string(e.Raw))
			}
		}
	}()

	ty.ctx, ty.cancel = context.WithCancel(context.Background())
	defer ty.cancel()	

	// 공개 WS
	ty.UpdateSymbols(ty.Symbols)


	// 유저 WS (listenKey 있을 때만)
	if ty.listenKey != "" {
		ty.UpdateListenKey(ty.listenKey)
	}

}


// 심볼 추가하기 
func (ty *FuturePriceStatus) AddSymbol(symbol string) {
	ty.Symbols = append(ty.Symbols, symbol)
	ty.UpdateSymbols(ty.Symbols)
}

// 심볼 빼기 
func (ty *FuturePriceStatus) RemoveSymbol(symbol string) {
	for i, s := range ty.Symbols {
		if s == symbol {
			ty.Symbols = append(ty.Symbols[:i], ty.Symbols[i+1:]...)
			break
		}
	}

	ty.UpdateSymbols(ty.Symbols)
}

// 심볼 변경 요청 처리 함수
func (ty *FuturePriceStatus) UpdateSymbols(newSymbols []string) {
    ty.mu.Lock()
    ty.Symbols = newSymbols
    ty.mu.Unlock()

    // 먼저 기존 context cancel로 WS 종료
    ty.cancel()

    // 새로운 context 생성해서 다시 실행
    ctx, cancel := context.WithCancel(context.Background())
    ty.ctx = ctx
    ty.cancel = cancel

    go func() {
        for {
            if err := runPublicWS(ty.ctx, ty.hub, ty.isFutures, ty.Symbols); err != nil {
                log.Printf("[public] error: %v (reconnect in 2s)", err)
                time.Sleep(2 * time.Second)
                continue
            }
            return
        }
    }()
}


func (ty *FuturePriceStatus) UpdateListenKey(newListenKey string) {
    if ty.listenKey != "" {
		go func() {
			for {
				if err := runUserWS(ty.ctx, ty.hub, ty.isFutures, ty.listenKey); err != nil {
					log.Printf("[user] error: %v (reconnect in 2s)", err)
					time.Sleep(2 * time.Second)
					continue
				}
				return
			}
		}()
	}

}


func (ty *FuturePriceStatus) GetPrice(symbol string, position string) (float64, bool) {
	tick, ok := ty.store.Get(symbol)
	sendPrice := 0.0
	if position == "LONG " {
		sendPrice = tick.Ask
	}else if position == "SHORT" {
		sendPrice = tick.Bid
	}

	if !ok {
		return sendPrice, false
	}

	return sendPrice, true


}

