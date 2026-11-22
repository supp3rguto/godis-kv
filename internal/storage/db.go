package storage

import (
	"fmt"
	"sync"
	"time"
)

// item representa o valor armazenado e seu tempo de vida útil.
type item struct {
	value     string
	expiresAt int64 // 0 significa que nunca expira
}

// DB representa o banco de dados em memória.
type DB struct {
	mu   sync.RWMutex
	data map[string]item
}

// NewDB inicializa um novo banco de dados e inicia a rotina de limpeza de TTL.
func NewDB() *DB {
	db := &DB{
		data: make(map[string]item),
	}
	go db.startCleanupRoutine()
	return db
}

// Set armazena uma chave. Passe ttlSeconds = 0 para chaves permanentes.
func (db *DB) Set(key, value string, ttlSeconds int) {
	db.mu.Lock()
	defer db.mu.Unlock()

	var expiresAt int64
	if ttlSeconds > 0 {
		expiresAt = time.Now().Add(time.Duration(ttlSeconds) * time.Second).UnixNano()
	}

	db.data[key] = item{value: value, expiresAt: expiresAt}
}

func (db *DB) Get(key string) (string, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	val, exists := db.data[key]
	if !exists {
		return "", false
	}

	if val.expiresAt > 0 && time.Now().UnixNano() > val.expiresAt {
		return "", false
	}

	return val.value, true
}

func (db *DB) Delete(key string) bool {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, exists := db.data[key]; exists {
		delete(db.data, key)
		return true
	}
	return false
}

// SnapshotCommands gera os comandos mínimos para reconstruir apenas as chaves vivas.
func (db *DB) SnapshotCommands() []string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	now := time.Now().UnixNano()
	commands := make([]string, 0, len(db.data))

	for k, v := range db.data {
		if v.expiresAt == 0 {
			commands = append(commands, fmt.Sprintf("SET %s %s", k, v.value))
			continue
		}

		remaining := v.expiresAt - now
		if remaining <= 0 {
			continue
		}

		// Arredonda para cima para garantir ao menos 1 segundo de TTL restante
		ttlSec := int((remaining + int64(time.Second) - 1) / int64(time.Second))
		commands = append(commands, fmt.Sprintf("SETEX %s %d %s", k, ttlSec, v.value))
	}

	return commands
}

func (db *DB) startCleanupRoutine() {
	ticker := time.NewTicker(1 * time.Second)
	for range ticker.C {
		db.mu.Lock()
		now := time.Now().UnixNano()
		for k, v := range db.data {
			if v.expiresAt > 0 && now > v.expiresAt {
				delete(db.data, k)
			}
		}
		db.mu.Unlock()
	}
}