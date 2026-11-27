package server_test

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/supp3rguto/godis-kv/internal/pubsub"
	"github.com/supp3rguto/godis-kv/internal/server"
	"github.com/supp3rguto/godis-kv/internal/storage"
)

func reserveAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to reserve local port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func dialWithRetry(t *testing.T, addr string) net.Conn {
	t.Helper()
	var (
		conn net.Conn
		err  error
	)
	for i := 0; i < 30; i++ {
		conn, err = net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			return conn
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("failed to connect to %s: %v", addr, err)
	return nil
}

func sendCmd(t *testing.T, conn net.Conn, reader *bufio.Reader, cmd string) string {
	t.Helper()
	if _, err := conn.Write([]byte(cmd + "\n")); err != nil {
		t.Fatalf("failed to write %q: %v", cmd, err)
	}
	resp, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read response for %q: %v", cmd, err)
	}
	return strings.TrimSpace(resp)
}

func TestIntegration_PersistenceCompactionPubSubAndRestart(t *testing.T) {
	aofPath := filepath.Join(t.TempDir(), "integration.log")

	//1--sobe instancia 1 real (DB + AOF + PubSub + TCP)
	aof1, err := storage.NewAOF(aofPath)
	if err != nil {
		t.Fatalf("failed to create AOF: %v", err)
	}
	db1 := storage.NewDB()
	broker1 := pubsub.NewBroker()
	addr1 := reserveAddr(t)
	srv1 := server.NewServer(addr1, db1, aof1, broker1)

	errCh1 := make(chan error, 1)
	go func() { errCh1 <- srv1.Start() }()

	conn1 := dialWithRetry(t, addr1)
	reader1 := bufio.NewReader(conn1)

	// 30 sobrescritas na mesma chave para inflar o AOF
	for i := 0; i < 30; i++ {
		if got := sendCmd(t, conn1, reader1, fmt.Sprintf("SET counter v%d", i)); got != "OK" {
			t.Fatalf("SET counter failed: %q", got)
		}
	}

	if got := sendCmd(t, conn1, reader1, "SET temp discard_me"); got != "OK" {
		t.Fatalf("SET temp failed: %q", got)
	}
	if got := sendCmd(t, conn1, reader1, "DEL temp"); got != "1" {
		t.Fatalf("DEL temp failed: %q", got)
	}
	if got := sendCmd(t, conn1, reader1, "SETEX session 60 tok123"); got != "OK" {
		t.Fatalf("SETEX session failed: %q", got)
	}

	beforeStat, err := os.Stat(aofPath)
	if err != nil {
		t.Fatalf("stat before compaction failed: %v", err)
	}

	// disparaa compactacao real em disco e aguarda reducao de tamanho
	if got := sendCmd(t, conn1, reader1, "BGREWRITEAOF"); got != "Background append only file rewriting started" {
		t.Fatalf("unexpected BGREWRITEAOF response: %q", got)
	}

	compacted := false
	for i := 0; i < 50; i++ {
		if st, err := os.Stat(aofPath); err == nil && st.Size() < beforeStat.Size() {
			compacted = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !compacted {
		t.Fatal("AOF file did not shrink after BGREWRITEAOF")
	}

	// Pub/Sub ponta a ponta entre dois clientes TCP reais
	subConn := dialWithRetry(t, addr1)
	subReader := bufio.NewReader(subConn)

	if got := sendCmd(t, subConn, subReader, "SUBSCRIBE events"); got != "SUBSCRIBED events" {
		t.Fatalf("SUBSCRIBE failed: %q", got)
	}
	if got := sendCmd(t, conn1, reader1, "PUBLISH events system ready"); got != "1" {
		t.Fatalf("PUBLISH expected 1 delivery, got %q", got)
	}
	msg, err := subReader.ReadString('\n')
	if err != nil || strings.TrimSpace(msg) != "MESSAGE events system ready" {
		t.Fatalf("unexpected Pub/Sub message: (%q, %v)", msg, err)
	}

	// final limpo da instancia 1
	_ = subConn.Close()
	_ = conn1.Close()
	srv1.Stop()
	if err := <-errCh1; err != nil {
		t.Fatalf("server 1 stopped with error: %v", err)
	}
	if err := aof1.Close(); err != nil {
		t.Fatalf("failed closing AOF 1: %v", err)
	}

	// 2-- reinicia instancia 2 lendo o mesmo AOF compactado
	aof2, err := storage.NewAOF(aofPath)
	if err != nil {
		t.Fatalf("failed to reopen AOF: %v", err)
	}
	defer aof2.Close()

	db2 := storage.NewDB()
	if err := aof2.Replay(db2); err != nil {
		t.Fatalf("AOF replay on restart failed: %v", err)
	}

	addr2 := reserveAddr(t)
	srv2 := server.NewServer(addr2, db2, aof2, pubsub.NewBroker())

	errCh2 := make(chan error, 1)
	go func() { errCh2 <- srv2.Start() }()

	conn2 := dialWithRetry(t, addr2)
	reader2 := bufio.NewReader(conn2)

	if got := sendCmd(t, conn2, reader2, "GET counter"); got != "v29" {
		t.Errorf("after restart: want counter='v29', got %q", got)
	}
	if got := sendCmd(t, conn2, reader2, "GET temp"); got != "(nil)" {
		t.Errorf("after restart: want temp='(nil)', got %q", got)
	}
	if got := sendCmd(t, conn2, reader2, "GET session"); got != "tok123" {
		t.Errorf("after restart: want session='tok123', got %q", got)
	}

	_ = conn2.Close()
	srv2.Stop()
	if err := <-errCh2; err != nil {
		t.Fatalf("server 2 stopped with error: %v", err)
	}
}