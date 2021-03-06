package main

import (
	"fmt"
	"log"
	"time"
)

const (
	maxPending = 20
)

type Item struct {
	ID string `json:"id"`
}

func Fetch(url string) ([]Item, time.Time, error) {
	log.Println("Fetch invoke")
	<-time.After(time.Second)
	return []Item{}, time.Now().Add(3 * time.Second), fmt.Errorf("still error from fetch")
}

func main() {

	h := &Handler{
		updates: make(chan Item),
		closing: make(chan chan error),
	}

	done := make(chan struct{})
	defer close(done)

	go func() {
		for x := range h.GetUpdates() {
			log.Printf("-> %v", x)
		}
	}()

	h.IOLoop(done)
}

type Handler struct {
	updates chan Item
	closing chan chan error
}

func (h *Handler) Close() error {
	errCh := make(chan error)
	h.closing <- errCh
	return <-errCh
}

func (h *Handler) GetUpdates() chan Item {
	return h.updates
}

type fetchResult struct {
	fetched []Item
	next    time.Time
	err     error
}

func (h *Handler) IOLoop(done <-chan struct{}) {
	var (
		err     error
		next    time.Time // 下次再 fetch 时间点
		pending []Item

		// 给 Fetch 去重 (deduplicat)
		seen = make(map[string]bool)
		// if non-nil, Fetch is running
		fetchDone chan fetchResult
	)

	for {
		// 下一次执行 fetch 需要延迟多久
		var fetchDelay time.Duration
		if now := time.Now(); next.After(now) { // next 还没到，需要延迟
			fetchDelay = next.Sub(now)
		}

		// fix: 限制 pending 无限膨胀
		// 方法1，进一步修改 fetch 开始时机，不仅要有 delay 限制，还要检查 pending 限制，同时还要检查异步 Fetch 执行状态
		// 方法2，丢弃 pending 最早的 item，满足 maxPending 限制
		var startFetch <-chan time.Time
		if fetchDone == nil && len(pending) > maxPending {
			startFetch = time.After(fetchDelay)
		}

		var firstItem Item
		var updates chan Item
		if len(pending) > 0 {
			firstItem = pending[0]
			updates = h.updates
		}

		// 所有的修改和优化都朝着同一个方向：尽量减少每一个 case 的时间占用，
		// 让一个 goroutine 就可以将 for loop 高效 run 起来
		select {
		case errCh := <-h.closing:
			errCh <- err
			close(h.updates)
			return
		case <-startFetch:
			// 是时候开始下一次 fetch 了
			fetchDone = make(chan fetchResult, 1)
			go func() {
				fetched, next, err := Fetch("")
				fetchDone <- fetchResult{fetched, next, err}
			}()
		case result := <-fetchDone:
			fetchDone = nil // 此次 Fetch 执行完毕，可以开始下一次
			var fetched []Item
			fetched, next, err = result.fetched, result.next, result.err
			if err != nil {
				// 出错需要延迟 5s 再试
				next = time.Now().Add(5 * time.Second)
				log.Printf("fetching fail: %v", err)
				break // 跳出 select, 继续 for
			}
			for _, item := range fetched {
				// 利用 map 去重
				if !seen[item.ID] {
					pending = append(pending, item)
					seen[item.ID] = true
				}
			}
		case updates <- firstItem:
			// send out
			pending = pending[1:]
		case <-time.After(5 * time.Second):
			log.Println("5s timeout")
		case <-done:
			// TODO: 似乎和 closing 冲突了
			return
		}

		log.Println("end of for loop")
	}
}
