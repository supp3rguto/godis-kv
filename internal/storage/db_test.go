package storage

import (
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDB_SetGetDelete(t *testing.T) {
	db := NewDB()

	if _, exists := db.Get("nonexistent"); exists {
		t.Fatal("expected false for nonexistent key")
	}

	db.Set("language", "golang", 0)
	val, exists := db.Get("language")
	if !exists || val != "golang" {
		t.Fatalf("want ('golang', true), got (%q, %v)", val, exists)
	}

	db.Set("language", "go 1.26", 0)
	val, exists = db.Get("language")
	if !exists || val != "go 1.26" {
		t.Fatalf("want ('go 1.26', true), got (%q, %v)", val, exists)
	}

	if deleted := db.Delete("language"); !deleted {
		t.Fatal("expected true when deleting existing key")
	}
	if deleted := db.Delete("language"); deleted {
		t.Fatal("expected false when deleting already removed key")
	}
	if _, exists := db.Get("language"); exists {
		t.Fatal("deleted key should not exist")
	}
}

func TestDB_TTLExpiration(t *testing.T) {
	db := NewDB()
	db.Set("session", "active", 1)

	val, exists := db.Get("session")
	if !exists || val != "active" {
		t.Fatalf("key should exist before TTL expires; got (%q, %v)", val, exists)
	}

	time.Sleep(1100 * time.Millisecond)

	if _, exists := db.Get("session"); exists {
		t.Fatal("key should have expired after TTL")
	}
}

func TestDB_SnapshotCommands(t *testing.T) {
	// Struct direta sem goroutine de limpeza para testar o filtro de expiradas dentro do SnapshotCommands
	db := &DB{data: make(map[string]item)}

	db.Set("perm", "valor fixo", 0)
	db.Set("live_ttl", "ainda_vivo", 60)
	db.data["expired"] = item{
		value:     "morto",
		expiresAt: time.Now().Add(-1 * time.Second).UnixNano(),
	}

	cmds := db.SnapshotCommands()
	if len(cmds) != 2 {
		t.Fatalf("want 2 live commands, got %d: %v", len(cmds), cmds)
	}

	var foundPerm, foundTTL bool
	for _, cmd := range cmds {
		switch {
		case cmd == "SET perm valor fixo":
			foundPerm = true
		case strings.HasPrefix(cmd, "SETEX live_ttl ") && strings.HasSuffix(cmd, " ainda_vivo"):
			foundTTL = true
		default:
			t.Errorf("unexpected snapshot command: %q", cmd)
		}
	}

	if !foundPerm || !foundTTL {
		t.Errorf("expected both permanent and TTL keys in snapshot, got: %v", cmds)
	}
}

func TestDB_RaceCondition(t *testing.T) {
	db := NewDB()
	var wg sync.WaitGroup

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			db.Set("hot_key", "value", 0)
			db.Get("hot_key")
			if id%10 == 0 {
				db.Delete("hot_key")
			}
		}(i)
	}

	wg.Wait()
}

func BenchmarkDB_Set(b *testing.B) {
	db := &DB{data: make(map[string]item)}
	keys := make([]string, 1024)
	for i := range keys {
		keys[i] = "key_" + strconv.Itoa(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Set(keys[i&1023], "benchmark_payload_value", 0)
	}
}

func BenchmarkDB_Get(b *testing.B) {
	db := &DB{data: make(map[string]item)}
	keys := make([]string, 1024)
	for i := range keys {
		keys[i] = "key_" + strconv.Itoa(i)
		db.Set(keys[i], "benchmark_payload_value", 0)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = db.Get(keys[i&1023])
	}
}

func BenchmarkDB_ReadHeavyParallel(b *testing.B) {
	db := &DB{data: make(map[string]item)}
	keys := make([]string, 1024)
	for i := range keys {
		keys[i] = "key_" + strconv.Itoa(i)
		db.Set(keys[i], "benchmark_payload_value", 0)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			k := keys[i&1023]
			if i%20 == 0 { // 5% escritas, 95% leituras (cenário ideal para RWMutex)
				db.Set(k, "updated_payload", 0)
			} else {
				_, _ = db.Get(k)
			}
			i++
		}
	})
}