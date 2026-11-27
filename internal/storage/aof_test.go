package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestAOF_AppendAndReplay(t *testing.T) {
	aofPath := filepath.Join(t.TempDir(), "nested", "database.log")

	aof, err := NewAOF(aofPath)
	if err != nil {
		t.Fatalf("failed to create AOF: %v", err)
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
			t.Fatalf("failed to append %q: %v", cmd, err)
		}
	}

	if err := aof.Close(); err != nil {
		t.Fatalf("failed to close AOF: %v", err)
	}

	reopenedAOF, err := NewAOF(aofPath)
	if err != nil {
		t.Fatalf("failed to reopen AOF: %v", err)
	}
	defer reopenedAOF.Close()

	db := NewDB()
	if err := reopenedAOF.Replay(db); err != nil {
		t.Fatalf("AOF Replay failed: %v", err)
	}

	if val, ok := db.Get("usuario"); !ok || val != "guto" {
		t.Errorf("want ('guto', true), got (%q, %v)", val, ok)
	}
	if val, ok := db.Get("frase"); !ok || val != "ola mundo em go" {
		t.Errorf("want ('ola mundo em go', true), got (%q, %v)", val, ok)
	}
	if _, ok := db.Get("temporaria"); ok {
		t.Error("key 'temporaria' should not exist after DEL in replay")
	}
	if val, ok := db.Get("token"); !ok || val != "segredo123" {
		t.Errorf("want ('segredo123', true), got (%q, %v)", val, ok)
	}
}

func TestAOF_Rewrite(t *testing.T) {
	aofPath := filepath.Join(t.TempDir(), "database.log")

	aof, err := NewAOF(aofPath)
	if err != nil {
		t.Fatalf("failed to create AOF: %v", err)
	}

	db := NewDB()

	for i := 0; i < 50; i++ {
		val := fmt.Sprintf("versao_%d", i)
		db.Set("contador", val, 0)
		if err := aof.Append("SET contador " + val); err != nil {
			t.Fatalf("append failed: %v", err)
		}
	}

	db.Set("lixo", "temporario", 0)
	_ = aof.Append("SET lixo temporario")
	db.Delete("lixo")
	_ = aof.Append("DEL lixo")

	db.Set("sessao", "abc123", 60)
	_ = aof.Append("SETEX sessao 60 abc123")

	beforeStat, err := os.Stat(aofPath)
	if err != nil {
		t.Fatalf("stat before rewrite failed: %v", err)
	}

	if err := aof.Rewrite(db.SnapshotCommands); err != nil {
		t.Fatalf("Rewrite failed: %v", err)
	}

	afterStat, err := os.Stat(aofPath)
	if err != nil {
		t.Fatalf("stat after rewrite failed: %v", err)
	}

	if afterStat.Size() >= beforeStat.Size() {
		t.Errorf("expected smaller file after rewrite: before=%d bytes, after=%d bytes", beforeStat.Size(), afterStat.Size())
	}

	db.Set("pos_rewrite", "ativo", 0)
	if err := aof.Append("SET pos_rewrite ativo"); err != nil {
		t.Fatalf("append after rewrite failed: %v", err)
	}

	if err := aof.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	reopened, err := NewAOF(aofPath)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer reopened.Close()

	restoredDB := NewDB()
	if err := reopened.Replay(restoredDB); err != nil {
		t.Fatalf("post-rewrite Replay failed: %v", err)
	}

	if val, ok := restoredDB.Get("contador"); !ok || val != "versao_49" {
		t.Errorf("want ('versao_49', true), got (%q, %v)", val, ok)
	}
	if _, ok := restoredDB.Get("lixo"); ok {
		t.Error("deleted key 'lixo' should not exist after rewrite")
	}
	if val, ok := restoredDB.Get("sessao"); !ok || val != "abc123" {
		t.Errorf("want ('abc123', true), got (%q, %v)", val, ok)
	}
	if val, ok := restoredDB.Get("pos_rewrite"); !ok || val != "ativo" {
		t.Errorf("want ('ativo', true), got (%q, %v)", val, ok)
	}
}

func TestAOF_ClosedErrors(t *testing.T) {
	aofPath := filepath.Join(t.TempDir(), "closed.log")
	aof, err := NewAOF(aofPath)
	if err != nil {
		t.Fatalf("failed to create AOF: %v", err)
	}

	if err := aof.Close(); err != nil {
		t.Fatalf("first Close failed: %v", err)
	}
	if err := aof.Close(); err != nil {
		t.Fatalf("second Close should be nil, got: %v", err)
	}

	if err := aof.Append("SET a b"); err == nil {
		t.Error("expected error on Append after Close")
	}
	if err := aof.Replay(&DB{data: make(map[string]item)}); err == nil {
		t.Error("expected error on Replay after Close")
	}
	if err := aof.Rewrite(func() []string { return nil }); err == nil {
		t.Error("expected error on Rewrite after Close")
	}
}

func TestAOF_ReplayMalformedLines(t *testing.T) {
	aofPath := filepath.Join(t.TempDir(), "malformed.log")
	raw := "\n   \nSET incomplete\nSETEX bad_ttl not_a_number val\nDEL\nGARBAGE line\nSET valid ok_value\n"
	if err := os.WriteFile(aofPath, []byte(raw), 0666); err != nil {
		t.Fatalf("failed to seed malformed log: %v", err)
	}

	aof, err := NewAOF(aofPath)
	if err != nil {
		t.Fatalf("failed to open AOF: %v", err)
	}
	defer aof.Close()

	db := &DB{data: make(map[string]item)}
	if err := aof.Replay(db); err != nil {
		t.Fatalf("Replay should tolerate malformed lines, got error: %v", err)
	}

	if val, ok := db.Get("valid"); !ok || val != "ok_value" {
		t.Fatalf("want ('ok_value', true), got (%q, %v)", val, ok)
	}
}

func TestAOF_ConcurrentAppendAndRewrite(t *testing.T) {
	aofPath := filepath.Join(t.TempDir(), "concurrent.log")
	aof, err := NewAOF(aofPath)
	if err != nil {
		t.Fatalf("failed to create AOF: %v", err)
	}
	defer aof.Close()

	db := NewDB()
	var wg sync.WaitGroup

	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 40; i++ {
				key := fmt.Sprintf("k_%d_%d", id, i%5)
				val := fmt.Sprintf("v_%d", i)
				db.Set(key, val, 0)
				if err := aof.Append(fmt.Sprintf("SET %s %s", key, val)); err != nil {
					t.Errorf("concurrent Append failed: %v", err)
				}
			}
		}(w)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			if err := aof.Rewrite(db.SnapshotCommands); err != nil {
				t.Errorf("concurrent Rewrite failed: %v", err)
			}
		}
	}()

	wg.Wait()
}

func FuzzAOF_Replay(f *testing.F) {
	seeds := []string{
		"SET k v\n",
		"SETEX k 10 hello world\nDEL k\n",
		"SETEX k abc val\n",
		"   \n\t\n",
		"SET",
		"DEL",
		"\x00\xff\xfe",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	dir := f.TempDir()
	aofPath := filepath.Join(dir, "fuzz.log")

	f.Fuzz(func(t *testing.T, data string) {
		if len(data) > 4096 {
			return
		}
		if err := os.WriteFile(aofPath, []byte(data), 0666); err != nil {
			t.Fatalf("failed to write fuzz file: %v", err)
		}

		aof, err := NewAOF(aofPath)
		if err != nil {
			t.Fatalf("NewAOF failed: %v", err)
		}
		defer aof.Close()

		db := &DB{data: make(map[string]item)}
		_ = aof.Replay(db)
	})
}