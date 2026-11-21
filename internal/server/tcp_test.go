package server

import (
	"bufio"
	"net"
	"strings"
	"sync"
	"testing"
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

type mockAOF struct {
	mu       sync.Mutex
	commands []string
}

func (m *mockAOF) Append(command string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.commands = append(m.commands, command)
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

	// encerra conexao do cliente e aguarda handler finalizar para evitar race nas assercoes
	clientConn.Close()
	<-done

	// valida que apenas comandos de escrita efetivos foram enviados ao AOF
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

	// valida que o TTL do SETEX foi repassado corretamente ao storage
	if gotTTL := store.ttls["cache"]; gotTTL != 30 {
		t.Errorf("TTL da chave 'cache': esperado 30, recebido %d", gotTTL)
	}
}