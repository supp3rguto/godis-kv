package server

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/supp3rguto/godis-kv/internal/pubsub"
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
	srv := NewServer(":0", store, aof, nil)

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
		{"SET incompleto", "ERR syntax: SET <key> <value>"},
		{"SETEX chave abc valor", "ERR invalid seconds"},
		{"SETEX chave 0 valor", "ERR invalid seconds"},
		{"GET", "ERR syntax: GET <key>"},
		{"DEL", "ERR syntax: DEL <key>"},
		{"PING", "ERR unknown command"},
	}

	reader := bufio.NewReader(clientConn)

	for _, tc := range tests {
		if _, err := clientConn.Write([]byte(tc.input + "\n")); err != nil {
			t.Fatalf("failed to send command %q: %v", tc.input, err)
		}

		got, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("failed to read response for %q: %v", tc.input, err)
		}

		got = strings.TrimSpace(got)
		if got != tc.want {
			t.Errorf("command %q: want %q, got %q", tc.input, tc.want, got)
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
		t.Fatalf("want %d AOF commands, got %d: %v", len(expectedAOF), len(aof.commands), aof.commands)
	}
	for i, cmd := range expectedAOF {
		if aof.commands[i] != cmd {
			t.Errorf("AOF[%d]: want %q, got %q", i, cmd, aof.commands[i])
		}
	}

	if gotTTL := store.ttls["cache"]; gotTTL != 30 {
		t.Errorf("TTL for 'cache': want 30, got %d", gotTTL)
	}
}

func TestServer_BGRewriteAOF(t *testing.T) {
	store := newMockStore()
	aof := &mockAOF{rewriteCalled: make(chan []string, 1)}
	srv := NewServer(":0", store, aof, nil)

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
		t.Fatalf("failed to read BGREWRITEAOF response: %v", err)
	}
	if strings.TrimSpace(resp) != "Background append only file rewriting started" {
		t.Fatalf("unexpected response: %q", resp)
	}

	select {
	case cmds := <-aof.rewriteCalled:
		if len(cmds) != 1 || cmds[0] != "SET chave1 valor1" {
			t.Errorf("unexpected rewrite snapshot: %v", cmds)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for background BGREWRITEAOF execution")
	}

	clientConn.Close()
	<-done
}

func TestServer_PubSub(t *testing.T) {
	store := newMockStore()
	broker := pubsub.NewBroker()
	srv := NewServer(":0", store, nil, broker)

	subClient, subServer := net.Pipe()
	pubClient, pubServer := net.Pipe()

	subDone := make(chan struct{})
	pubDone := make(chan struct{})

	go func() {
		defer close(subDone)
		srv.handleConnection(subServer)
	}()
	go func() {
		defer close(pubDone)
		srv.handleConnection(pubServer)
	}()

	subReader := bufio.NewReader(subClient)
	pubReader := bufio.NewReader(pubClient)

	if _, err := subClient.Write([]byte("SUBSCRIBE alertas\n")); err != nil {
		t.Fatalf("failed to send SUBSCRIBE: %v", err)
	}
	resp, err := subReader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read SUBSCRIBE response: %v", err)
	}
	if strings.TrimSpace(resp) != "SUBSCRIBED alertas" {
		t.Fatalf("want 'SUBSCRIBED alertas', got %q", resp)
	}

	if _, err := pubClient.Write([]byte("PUBLISH alertas servidor online\n")); err != nil {
		t.Fatalf("failed to send PUBLISH: %v", err)
	}
	pubResp, err := pubReader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read PUBLISH response: %v", err)
	}
	if strings.TrimSpace(pubResp) != "1" {
		t.Fatalf("want '1' delivery, got %q", pubResp)
	}

	msg, err := subReader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read published message: %v", err)
	}
	if strings.TrimSpace(msg) != "MESSAGE alertas servidor online" {
		t.Fatalf("want 'MESSAGE alertas servidor online', got %q", msg)
	}

	if _, err := subClient.Write([]byte("UNSUBSCRIBE alertas\n")); err != nil {
		t.Fatalf("failed to send UNSUBSCRIBE: %v", err)
	}
	unsubResp, err := subReader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read UNSUBSCRIBE response: %v", err)
	}
	if strings.TrimSpace(unsubResp) != "UNSUBSCRIBED alertas" {
		t.Fatalf("want 'UNSUBSCRIBED alertas', got %q", unsubResp)
	}

	if _, err := pubClient.Write([]byte("PUBLISH alertas ninguem ouvindo\n")); err != nil {
		t.Fatalf("failed to send second PUBLISH: %v", err)
	}
	pubResp, err = pubReader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read second PUBLISH response: %v", err)
	}
	if strings.TrimSpace(pubResp) != "0" {
		t.Fatalf("want '0' deliveries after unsubscribe, got %q", pubResp)
	}

	subClient.Close()
	pubClient.Close()
	<-subDone
	<-pubDone
}