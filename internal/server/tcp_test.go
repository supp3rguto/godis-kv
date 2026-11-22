package server

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

type mockStore struct {
	mu   sync.Mutex
	data map[string]string
	ttls map[string]int
}

func newMockStore() *mockStore {
	return &mockStore{
		data: make(map[string]string),
		ttls: make(map[string]int),
	}
}

func (m *mockStore) Set(key, value string, ttlSeconds int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = value
	m.ttls[key] = ttlSeconds
}

func (m *mockStore) Get(key string) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	val, ok := m.data[key]
	return val, ok
}

func (m *mockStore) Delete(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.data[key]; ok {
		delete(m.data, key)
		return true
	}
	return false
}

func (m *mockStore) SnapshotCommands() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	cmds := make([]string, 0, len(m.data))
	for k, v := range m.data {
		cmds = append(cmds, fmt.Sprintf("SET %s %s", k, v))
	}
	return cmds
}

type mockAOF struct {
	mu            sync.Mutex
	commands      []string
	rewriteCalled chan []string
}

func (m *mockAOF) Append(command string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.commands = append(m.commands, command)
	return nil
}

func (m *mockAOF) Rewrite(snapshotFn func() []string) error {
	cmds := snapshotFn()
	if m.rewriteCalled != nil {
		m.rewriteCalled <- cmds
	}
	return nil
}

func TestServer_ProtocolCommands(t *testing.T) {
	store := newMockStore()
	aof := &mockAOF{}
	srv := NewServer(":0", store, aof)

	clientConn, serverConn := net.Pipe()
	done := make(chan struct{})

	go func() {
		defer close(done)
		srv.handleConnection(serverConn)
	}()

	tests := []struct {
		input string
		want  string
	}{
		{"SET user guto", "OK"},
		{"GET user", "guto"},
		{"SET frase ola mundo", "OK"},
		{"GET frase", "ola mundo"},
		{"SETEX cache 30 valor_temp", "OK"},
		{"GET cache", "valor_temp"},
		{"DEL user", "1"},
		{"DEL user", "0"},
		{"GET user", "(nil)"},
		{"SET incompleto", "ERR Sintaxe: SET <chave> <valor>"},
		{"SETEX chave abc valor", "ERR Segundos invalidos"},
		{"SETEX chave 0 valor", "ERR Segundos invalidos"},
		{"GET", "ERR Sintaxe: GET <chave>"},
		{"DEL", "ERR Sintaxe: DEL <chave>"},
		{"PING", "ERR Comando desconhecido"},
	}

	reader := bufio.NewReader(clientConn)

	for _, tc := range tests {
		if _, err := clientConn.Write([]byte(tc.input + "\n")); err != nil {
			t.Fatalf("erro ao enviar comando %q: %v", tc.input, err)
		}

		got, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("erro ao ler resposta para %q: %v", tc.input, err)
		}

		got = strings.TrimSpace(got)
		if got != tc.want {
			t.Errorf("comando %q: esperado %q, recebido %q", tc.input, tc.want, got)
		}
	}

	clientConn.Close()
	<-done

	expectedAOF := []string{
		"SET user guto",
		"SET frase ola mundo",
		"SETEX cache 30 valor_temp",
		"DEL user",
	}

	if len(aof.commands) != len(expectedAOF) {
		t.Fatalf("esperava %d comandos no AOF, recebeu %d: %v", len(expectedAOF), len(aof.commands), aof.commands)
	}
	for i, cmd := range expectedAOF {
		if aof.commands[i] != cmd {
			t.Errorf("AOF[%d]: esperado %q, recebido %q", i, cmd, aof.commands[i])
		}
	}

	if gotTTL := store.ttls["cache"]; gotTTL != 30 {
		t.Errorf("TTL da chave 'cache': esperado 30, recebido %d", gotTTL)
	}
}

func TestServer_BGRewriteAOF(t *testing.T) {
	store := newMockStore()
	aof := &mockAOF{rewriteCalled: make(chan []string, 1)}
	srv := NewServer(":0", store, aof)

	clientConn, serverConn := net.Pipe()
	done := make(chan struct{})

	go func() {
		defer close(done)
		srv.handleConnection(serverConn)
	}()

	reader := bufio.NewReader(clientConn)

	_, _ = clientConn.Write([]byte("SET chave1 valor1\n"))
	_, _ = reader.ReadString('\n')

	_, _ = clientConn.Write([]byte("BGREWRITEAOF\n"))
	resp, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("erro ao ler resposta do BGREWRITEAOF: %v", err)
	}
	if strings.TrimSpace(resp) != "Background append only file rewriting started" {
		t.Fatalf("resposta inesperada: %q", resp)
	}

	select {
	case cmds := <-aof.rewriteCalled:
		if len(cmds) != 1 || cmds[0] != "SET chave1 valor1" {
			t.Errorf("snapshot inesperado no rewrite: %v", cmds)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout aguardando execucao do BGREWRITEAOF em background")
	}

	clientConn.Close()
	<-done
}