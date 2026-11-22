package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestAOF_AppendAndReplay(t *testing.T) {
	aofPath := filepath.Join(t.TempDir(), "nested", "database.log")

	aof, err := NewAOF(aofPath)
	if err != nil {
		t.Fatalf("erro ao criar AOF: %v", err)
	}

	commands := []string{
		"SET usuario guto",
		"SET frase ola mundo em go",
		"SET temporaria apagar_depois",
		"DEL temporaria",
		"SETEX token 60 segredo123",
	}

	for _, cmd := range commands {
		if err := aof.Append(cmd); err != nil {
			t.Fatalf("erro ao gravar comando %q: %v", cmd, err)
		}
	}

	if err := aof.Close(); err != nil {
		t.Fatalf("erro ao fechar AOF: %v", err)
	}

	reopenedAOF, err := NewAOF(aofPath)
	if err != nil {
		t.Fatalf("erro ao reabrir AOF: %v", err)
	}
	defer reopenedAOF.Close()

	db := NewDB()
	if err := reopenedAOF.Replay(db); err != nil {
		t.Fatalf("erro no Replay do AOF: %v", err)
	}

	if val, ok := db.Get("usuario"); !ok || val != "guto" {
		t.Errorf("esperava ('guto', true), recebeu (%q, %v)", val, ok)
	}

	if val, ok := db.Get("frase"); !ok || val != "ola mundo em go" {
		t.Errorf("esperava ('ola mundo em go', true), recebeu (%q, %v)", val, ok)
	}

	if _, ok := db.Get("temporaria"); ok {
		t.Error("chave 'temporaria' nao deveria existir apos DEL no replay")
	}

	if val, ok := db.Get("token"); !ok || val != "segredo123" {
		t.Errorf("esperava ('segredo123', true), recebeu (%q, %v)", val, ok)
	}
}

func TestAOF_Rewrite(t *testing.T) {
	aofPath := filepath.Join(t.TempDir(), "database.log")

	aof, err := NewAOF(aofPath)
	if err != nil {
		t.Fatalf("erro ao criar AOF: %v", err)
	}

	db := NewDB()

	// Simula 50 atualizacoes redundantes na mesma chave
	for i := 0; i < 50; i++ {
		val := fmt.Sprintf("versao_%d", i)
		db.Set("contador", val, 0)
		if err := aof.Append("SET contador " + val); err != nil {
			t.Fatalf("erro no append: %v", err)
		}
	}

	// Chave criada e deletada (deve sumir do log compactado)
	db.Set("lixo", "temporario", 0)
	_ = aof.Append("SET lixo temporario")
	db.Delete("lixo")
	_ = aof.Append("DEL lixo")

	// Chave com TTL ativo
	db.Set("sessao", "abc123", 60)
	_ = aof.Append("SETEX sessao 60 abc123")

	beforeStat, err := os.Stat(aofPath)
	if err != nil {
		t.Fatalf("erro ao inspecionar tamanho antes do rewrite: %v", err)
	}

	if err := aof.Rewrite(db.SnapshotCommands); err != nil {
		t.Fatalf("erro ao executar Rewrite: %v", err)
	}

	afterStat, err := os.Stat(aofPath)
	if err != nil {
		t.Fatalf("erro ao inspecionar tamanho depois do rewrite: %v", err)
	}

	if afterStat.Size() >= beforeStat.Size() {
		t.Errorf("esperava arquivo menor apos rewrite: antes=%d bytes, depois=%d bytes", beforeStat.Size(), afterStat.Size())
	}

	// Garante que o AOF continua aceitando novos comandos apos a troca do arquivo
	db.Set("pos_rewrite", "ativo", 0)
	if err := aof.Append("SET pos_rewrite ativo"); err != nil {
		t.Fatalf("erro ao gravar comando apos rewrite: %v", err)
	}

	if err := aof.Close(); err != nil {
		t.Fatalf("erro ao fechar AOF: %v", err)
	}

	// Reabre e valida o Replay do log compactado
	reopened, err := NewAOF(aofPath)
	if err != nil {
		t.Fatalf("erro ao reabrir AOF: %v", err)
	}
	defer reopened.Close()

	restoredDB := NewDB()
	if err := reopened.Replay(restoredDB); err != nil {
		t.Fatalf("erro no Replay pos-rewrite: %v", err)
	}

	if val, ok := restoredDB.Get("contador"); !ok || val != "versao_49" {
		t.Errorf("esperava ('versao_49', true), recebeu (%q, %v)", val, ok)
	}
	if _, ok := restoredDB.Get("lixo"); ok {
		t.Error("chave deletada 'lixo' nao deveria existir apos rewrite")
	}
	if val, ok := restoredDB.Get("sessao"); !ok || val != "abc123" {
		t.Errorf("esperava ('abc123', true), recebeu (%q, %v)", val, ok)
	}
	if val, ok := restoredDB.Get("pos_rewrite"); !ok || val != "ativo" {
		t.Errorf("esperava ('ativo', true), recebeu (%q, %v)", val, ok)
	}
}