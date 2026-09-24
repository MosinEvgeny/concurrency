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
	"sync"
	"time"
)

type Job struct {
	ID int
}

type Result struct {
	square int
	error  error
}

func main() {
	N := 10
	M := 5
	timeout := time.Second * 3

	jobs := make(chan Job)
	results := make(chan Result)

	ctx := context.Background()
	wg := &sync.WaitGroup{}

	go func() {
		for i := 1; i <= N; i++ {
			jobs <- Job{
				ID: i,
			}
		}
		close(jobs)
	}()

	for range M {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for job := range jobs {
				subCtx, cancel := context.WithTimeout(ctx, timeout)

				res := processJob(subCtx, job)
				cancel()

				results <- res
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	for res := range results {
		fmt.Printf("Результат: %d, ошибка: %v \n", res.square, res.error)
	}
}

func processJob(ctx context.Context, job Job) (res Result) {
	defer func() {
		if p := recover(); p != nil {
			res.error = fmt.Errorf("job %d: panic: %v", job.ID, p)
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
