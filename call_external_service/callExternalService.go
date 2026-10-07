// 1 Базовый пул воркеров
// Генератор (producer) кладёт `N` заданий (`Job{ID: 1..N}`) в канал `jobs`.
// `M` воркеров читают из `jobs`, обрабатывают через `callExternalService`, пишут `Result` в канал `results`.
// `main` собирает все результаты из `results` и печатает их.
// Завершиться корректно: дождаться всех воркеров и не уронить программу.

// 2 Потокобезопасная статистика + детектор гонок
// Добавить сборщик `Stats`: счётчики `panics` / `failed` и `map[workerID]int` (сколько заданий успешно сделал каждый воркер).
// Воркеры обновляют статистику конкурентно.

// 3 Ограничение параллелизма семафором
// Внешний сервис держит максимум `K` одновременных вызовов (например, `K=2` при `M=4` воркерах). Реализовать ограничение **семафором на буферизованном канале**.

// 4 Отмена через context + отсутствие утечек
// Протащить `context.Context` от `main` во все воркеры и в `callExternalService`.
// Завести общий таймаут операции через `context.WithTimeout` (например, 2–3 с).
// Воркеры обязаны **уважать** `ctx.Done()`: при отмене не начинать новую работу и не залипать.

// 5 Graceful shutdown
// Сервис должен корректно останавливаться по сигналу ОС (`SIGINT`/`SIGTERM`): перестать брать новые задания, дать текущим завершиться, но не висеть вечно.

package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type Job struct {
	ID int
}

type Result struct {
	square   int
	error    error
	panicked bool
}

type Stats struct {
	mu       sync.Mutex
	panics   int
	failed   int
	byWorker map[int]int
}

func (s *Stats) record(workerID int, res Result) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch {
	case res.panicked:
		s.panics++
	case res.error != nil:
		s.failed++
	default:
		s.byWorker[workerID]++
	}
}

func main() {
	jobCount := 10
	workerCount := 4
	maxConcurrent := 2
	timeout := 3 * time.Second
	shutdownTimeout := 2 * time.Second

	workCtx, cancelWork := context.WithTimeout(context.Background(), timeout)
	defer cancelWork()

	stopCtx, stop := signal.NotifyContext(workCtx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	jobs := make(chan Job)
	results := make(chan Result)
	semaphore := make(chan struct{}, maxConcurrent)
	statistics := &Stats{byWorker: make(map[int]int)}

	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		produceJobs(stopCtx, jobCount, jobs)
	}()

	var workers sync.WaitGroup
	for workerID := range workerCount {
		workers.Add(1)
		go func() {
			defer workers.Done()
			runWorker(workCtx, stopCtx, workerID, jobs, results, semaphore, statistics)
		}()
	}

	workersDone := make(chan struct{})
	go func() {
		workers.Wait()
		close(results)
		close(workersDone)
	}()

	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		waitForShutdown(workCtx, stopCtx, workersDone, shutdownTimeout, cancelWork)
	}()

	for res := range results {
		fmt.Printf("Результат: %d, ошибка: %v\n", res.square, res.error)
	}
	<-producerDone
	<-shutdownDone

	fmt.Printf("\nStatistics:\nPanics: %d\nFailed: %d\nBy worker: %v\n", statistics.panics, statistics.failed, statistics.byWorker)
}

func produceJobs(ctx context.Context, count int, jobs chan<- Job) {
	defer close(jobs)
	for id := 1; id <= count; id++ {
		select {
		case <-ctx.Done():
			return
		case jobs <- Job{ID: id}:
		}
	}
}

func runWorker(
	workCtx, stopCtx context.Context,
	workerID int,
	jobs <-chan Job,
	results chan<- Result,
	semaphore chan struct{},
	statistics *Stats,
) {
	for {
		var job Job
		select {
		case <-stopCtx.Done():
			return
		case next, ok := <-jobs:
			if !ok {
				return
			}
			job = next
		}

		select {
		case <-stopCtx.Done():
			return
		case semaphore <- struct{}{}:
		}

		if stopCtx.Err() != nil {
			<-semaphore
			return
		}

		res := processJob(workCtx, job)
		<-semaphore
		statistics.record(workerID, res)

		select {
		case <-workCtx.Done():
			return
		case results <- res:
		}
	}
}

func processJob(ctx context.Context, job Job) (res Result) {
	defer func() {
		if p := recover(); p != nil {
			res.error = fmt.Errorf("job %d: panic: %v", job.ID, p)
			res.panicked = true
		}
	}()
	res.square, res.error = callExternalService(ctx, job)
	return
}

func callExternalService(ctx context.Context, j Job) (int, error) {
	work := time.Duration(50+rand.Intn(250)) * time.Millisecond

	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-time.After(work):
	}

	roll := rand.Intn(10)
	switch {
	case roll == 0:
		panic(fmt.Sprintf("сервис паникнул на job %d", j.ID))
	case roll <= 2:
		return 0, fmt.Errorf("сервис вернул ошибку на job %d", j.ID)
	default:
		return j.ID * j.ID, nil
	}
}

func waitForShutdown(
	workCtx, stopCtx context.Context,
	workersDone <-chan struct{},
	timeout time.Duration,
	cancelWork context.CancelFunc,
) {
	select {
	case <-workersDone:
		return
	case <-workCtx.Done():
		return
	case <-stopCtx.Done():
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-workersDone:
	case <-workCtx.Done():
	case <-timer.C:
		cancelWork()
	}
}
