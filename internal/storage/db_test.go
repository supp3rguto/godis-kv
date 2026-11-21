package storage

import (
	"sync"
	"testing"
	"time"
)

// testa se a gravação basica e leitura estao funcionando
func TestDB_SetAndGet(t *testing.T) {
	db := NewDB()
	db.Set("linguagem", "golang", 0)

	val, exists := db.Get("linguagem")
	if !exists || val != "golang" {
		t.Errorf("Esperava 'golang', mas recebeu: '%v'", val)
	}
}

func TestDB_SetGetDelete(t *testing.T) {
	db := NewDB()

	// Chave inexistente
	if _, exists := db.Get("inexistente"); exists {
		t.Fatal("esperava false para chave inexistente")
	}

	// Set e Get
	db.Set("linguagem", "golang", 0)
	val, exists := db.Get("linguagem")
	if !exists || val != "golang" {
		t.Fatalf("esperava ('golang', true), recebeu (%q, %v)", val, exists)
	}

	// Sobrescrita de valor
	db.Set("linguagem", "go 1.26", 0)
	val, exists = db.Get("linguagem")
	if !exists || val != "go 1.26" {
		t.Fatalf("esperava ('go 1.26', true), recebeu (%q, %v)", val, exists)
	}

	// Delete em chave existente e inexistente
	if deleted := db.Delete("linguagem"); !deleted {
		t.Fatal("esperava true ao deletar chave existente")
	}
	if deleted := db.Delete("linguagem"); deleted {
		t.Fatal("esperava false ao deletar chave ja removida")
	}
	if _, exists := db.Get("linguagem"); exists {
		t.Fatal("chave deletada nao deveria existir")
	}
}

func TestDB_TTLExpiration(t *testing.T) {
	db := NewDB()
	db.Set("sessao", "ativa", 1)

	val, exists := db.Get("sessao")
	if !exists || val != "ativa" {
		t.Fatalf("chave deveria existir antes do TTL expirar; recebeu (%q, %v)", val, exists)
	}

	time.Sleep(1100 * time.Millisecond)

	if _, exists := db.Get("sessao"); exists {
		t.Fatal("chave deveria ter expirado apos o TTL")
	}
}

// teste principal - vai tentar causar uma "condicao de corrida" de poprosito
func TestDB_RaceCondition(t *testing.T) {
	db := NewDB()
	var wg sync.WaitGroup

	// Cria 1.000 clientes simultâneos
	numWorkers := 1000

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		
		go func(id int) {
			defer wg.Done()
			// todos tentando escrever e ler na exata mesma fracao de ms
			db.Set("chave_critica", "valor", 0)
			db.Get("chave_critica")
		}(i)
	}

	wg.Wait() // espera todas go routines terminarem
	// rmenber: se o banco nao der "panic" e quebrar por corrupção de memoria, o teste passa
}