package fun_in

import (
	"context"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

// etl_pipeline.go
//
//
//   genWeb   ─ normalize ─┐
//                         ├─ merge (fan-in) ─ Event ── batch(size + timeout) ─ consume
//   genApp   ─ normalize ─┘

// --- Структуры источников: разная форма, близкий смысл ---

type WebEvent struct {
	SessionID string
	URL       string
	TS        int64 // unix seconds
}

type AppEvent struct {
	DeviceID  string
	Screen    string
	EventTime time.Time
}

// --- Общий нормализованный вид ---

type Event struct {
	Source string // web | app
	UserID string // sessions | device
	Action string // url | screen
	At     time.Time
}

func normalizeWebEventToEvent(ctx context.Context, in <-chan WebEvent) chan Event {
	out := make(chan Event)

	go func() {
		defer close(out)

		for {
			select {
			case webEvent, ok := <-in:
				if !ok {
					return
				}
				event := Event{
					Source: "web",
					UserID: webEvent.SessionID,
					Action: webEvent.URL,
					At:     time.Unix(webEvent.TS, 0),
				}
				select {
				case out <- event:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return out
}

func normalizeAppEventToEvent(ctx context.Context, in <-chan AppEvent) chan Event {
	out := make(chan Event)

	go func() {
		defer close(out)

		for {
			select {
			case appEvent, ok := <-in:
				if !ok {
					return
				}
				event := Event{
					Source: "app",
					UserID: appEvent.DeviceID,
					Action: appEvent.Screen,
					At:     appEvent.EventTime,
				}
				select {
				case out <- event:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return out
}

func merge(ctx context.Context, first, second chan Event) chan Event {
	out := make(chan Event)
	wg := &sync.WaitGroup{}

	send := func(c chan Event) {
		defer wg.Done()

		for {
			select {
			case v, ok := <-c:
				if !ok {
					return
				}
				select {
				case out <- v:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}

	wg.Add(2)
	go send(first)
	go send(second)

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

// Доделать + во все методы добавить контекст
func batch(ctx context.Context, in chan Event, size uint64, timeout time.Duration) chan []Event {
	out := make(chan []Event)

	go func() {
		defer close(out)

		eventSlice := make([]Event, 0, size)
		timer := time.NewTimer(timeout)
		defer timer.Stop()

		flush := func() {
			if len(eventSlice) == 0 {
				return
			}

			select {
			case out <- eventSlice:
			case <-ctx.Done():
				return
			}
			eventSlice = make([]Event, 0, size)
		}

		for {
			select {
			case e, ok := <-in:
				if !ok {
					flush()
					return
				}
				eventSlice = append(eventSlice, e)
				if uint64(len(eventSlice)) == size {
					flush()
					timer.Stop()
					timer = time.NewTimer(timeout)
				}

			case <-timer.C:
				flush()
				timer = time.NewTimer(timeout)

			case <-ctx.Done():
				return
			}
		}
	}()

	return out
}

// --- Генераторы ---

func genWeb(ctx context.Context, every time.Duration) <-chan WebEvent {
	out := make(chan WebEvent)

	go func() {
		defer close(out)

		t := time.NewTicker(every)
		defer t.Stop()
		i := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				i++
				ev := WebEvent{
					SessionID: "web-" + strconv.Itoa(i),
					URL:       "/p/" + strconv.Itoa(rand.Intn(5)),
					TS:        time.Now().Unix(),
				}
				// Отправку тоже прикрываем ctx, иначе на отмене
				// горутина зависнет на out <-  ev, если читателя уже нет.
				select {
				case out <- ev:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return out
}

func genApp(ctx context.Context, every time.Duration) <-chan AppEvent {
	out := make(chan AppEvent)
	go func() {
		defer close(out)
		t := time.NewTicker(every)
		defer t.Stop()
		i := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				i++
				ev := AppEvent{
					DeviceID:  "dev-" + strconv.Itoa(i),
					Screen:    "screen_" + strconv.Itoa(rand.Intn(5)),
					EventTime: time.Now(),
				}
				select {
				case out <- ev:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}

func consume(in <-chan []Event) {
	n := 0
	for b := range in {
		n++
		web, app := 0, 0
		for _, e := range b {
			switch e.Source {
			case "web":
				web++
			case "app":
				app++
			}
		}
		log.Printf("batch #%d: %d событий (web=%d app=%d)", n, len(b), web, app)
	}
	log.Printf("готово, всего батчей: %d", n)
}
