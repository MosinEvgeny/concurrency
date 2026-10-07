package main

import (
	"fmt"
	"sync"
	"time"
)

// entry описывает одну запись кэша.
type entry struct {
	value     string
	expiresAt time.Time
}

// TTLCache — потокобезопасный кэш с ограниченным временем жизни записей.
type TTLCache struct {
	mu              sync.Mutex
	items           map[string]entry
	ttl             time.Duration
	cleanupInterval time.Duration
	stop            chan struct{}
	done            chan struct{}
	closeOnce       sync.Once
}

// NewTTLCache создаёт кэш и запускает фоновую горутину очистки.
func NewTTLCache(ttl time.Duration, cleanupInterval time.Duration) *TTLCache {
	// TODO: инициализировать поля структуры и запустить фоновую горутину очистки
	return nil
}

// Set сохраняет значение по ключу с временем жизни ttl.
func (c *TTLCache) Set(key string, value string) {
	// TODO: реализовать потокобезопасную запись значения
}

// Get возвращает значение и признак наличия неистёкшей записи.
func (c *TTLCache) Get(key string) (string, bool) {
	// TODO: реализовать потокобезопасное чтение с проверкой TTL
	return "", false
}

// cleanupLoop периодически удаляет просроченные записи.
func (c *TTLCache) cleanupLoop() {
	// TODO: реализовать цикл очистки с корректным завершением по сигналу stop
}

// removeExpired удаляет все просроченные записи.
func (c *TTLCache) removeExpired() {
	// TODO: реализовать удаление просроченных записей
}

// Close останавливает фоновую горутину; повторный вызов безопасен.
func (c *TTLCache) Close() {
	// TODO: реализовать идемпотентную остановку фоновой горутины
}

func main() {
	cache := NewTTLCache(50*time.Millisecond, 20*time.Millisecond)
	cache.Set("lada-vesta", "в наличии")
	v, ok := cache.Get("lada-vesta")
	fmt.Printf("v=%q, ok=%v\n", v, ok)
	time.Sleep(80 * time.Millisecond)
	v2, ok2 := cache.Get("lada-vesta")
	fmt.Printf("v2=%q, ok2=%v\n", v2, ok2)
	cache.Close()
	cache.Close()
}
