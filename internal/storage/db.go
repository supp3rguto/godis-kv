package storage

import (
	"sync"
	"time"
)

// item representa o valor armazenado e seu tempo de vida útil
type item struct {
	value     string
	expiresAt int64 // 0 significa que nunca expira
}

// DB representa o nosso banco de dados em memória
type DB struct {
	mu   sync.RWMutex
	data map[string]item
}

// NewDB inicializa um novo banco de dados e inicia a limpeza em background
func NewDB() *DB {
	db := &DB{
		data: make(map[string]item),
	}
	go db.startCleanupRoutine() // Inicia a Goroutine de limpeza
	return db
}

// Set agora aceita um TTL (em segundos). Passe 0 para chaves permanentes.
func (db *DB) Set(key, value string, ttlSeconds int) {
	db.mu.Lock()
	defer db.mu.Unlock()

	var expiresAt int64
	if ttlSeconds > 0 {
		// Calcula o momento exato no futuro em que a chave deve morrer (em nanosegundos)
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

	// Se a chave tiver tempo de expiração e o tempo atual já passou desse limite...
	if val.expiresAt > 0 && time.Now().UnixNano() > val.expiresAt {
		return "", false // Retorna que não existe (a rotina de limpeza vai apagar do mapa depois)
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

// startCleanupRoutine roda um loop infinito a cada 1 segundo apagando lixo da memória
func (db *DB) startCleanupRoutine() {
	ticker := time.NewTicker(1 * time.Second)
	for range ticker.C {
		db.mu.Lock() // Bloqueia para escrever/deletar
		now := time.Now().UnixNano()
		for k, v := range db.data {
			if v.expiresAt > 0 && now > v.expiresAt {
				delete(db.data, k)
			}
		}
		db.mu.Unlock()
	}
}