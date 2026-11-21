package storage

import (
	"path/filepath"
	"testing"
)

func TestAOF_AppendAndReplay(t *testing.T) {
	// caminho aninhado dentro de um diretorio temporario para validar os.MkdirAll
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

	// reabre o arquivo simulando reinicio do servidor
	reopenedAOF, err := NewAOF(aofPath)
	if err != nil {
		t.Fatalf("erro ao reabrir AOF: %v", err)
	}
	defer reopenedAOF.Close()

	db := NewDB()
	if err := reopenedAOF.Replay(db); err != nil {
		t.Fatalf("erro no Replay do AOF: %v", err)
	}

	// valida reconstrucao do estado em memoria
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