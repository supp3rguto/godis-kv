package storage

import (
	"sync"
	"testing"
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