package server

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// defines the in-memory storage contract consumed by the server.
type KeyValueStore interface {
	Set(key, value string, ttlSeconds int)
	Get(key string) (string, bool)
	Delete(key string) bool
	SnapshotCommands() []string
}

// . defines the disk persistence contract.
type AppendLogger interface {
	Append(command string) error
	Rewrite(snapshotFn func() []string) error
}

//defines the real-time messaging contract.
type PubSubBroker interface {
	Subscribe(channel string) (<-chan string, func())
	Publish(channel, message string) int
}

type Server struct {
	addr      string
	db        KeyValueStore
	aof       AppendLogger
	pubsub    PubSubBroker
	mu        sync.Mutex
	listener  net.Listener
	rewriting atomic.Bool
}

func NewServer(addr string, db KeyValueStore, aof AppendLogger, pubsub PubSubBroker) *Server {
	return &Server{
		addr:   addr,
		db:     db,
		aof:    aof,
		pubsub: pubsub,
	}
}

func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.listener = ln
	s.mu.Unlock()

	fmt.Printf("TCP server listening on %s\n", s.addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				break
			}
			log.Printf("Error accepting connection: %v", err)
			continue
		}
		go s.handleConnection(conn)
	}
	return nil
}

func (s *Server) Stop() {
	s.mu.Lock()
	ln := s.listener
	s.mu.Unlock()

	if ln != nil {
		ln.Close()
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	var (
		writeMu sync.Mutex
		wg      sync.WaitGroup
		subs    = make(map[string]func())
	)

	defer wg.Wait()
	defer conn.Close()
	defer func() {
		for _, unsub := range subs {
			unsub()
		}
	}()

	writeLine := func(msg string) {
		writeMu.Lock()
		defer writeMu.Unlock()
		_, _ = conn.Write([]byte(msg + "\n"))
	}

	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		cmdParts := strings.SplitN(line, " ", 2)
		command := strings.ToUpper(cmdParts[0])

		switch command {
		case "SET":
			parts := strings.SplitN(line, " ", 3)
			if len(parts) < 3 {
				writeLine("ERR syntax: SET <key> <value>")
				continue
			}
			s.db.Set(parts[1], parts[2], 0)
			s.logCommand(line)
			writeLine("OK")

		case "SETEX":
			parts := strings.SplitN(line, " ", 4)
			if len(parts) < 4 {
				writeLine("ERR syntax: SETEX <key> <seconds> <value>")
				continue
			}
			ttl, err := strconv.Atoi(parts[2])
			if err != nil || ttl <= 0 {
				writeLine("ERR invalid seconds")
				continue
			}
			s.db.Set(parts[1], parts[3], ttl)
			s.logCommand(line)
			writeLine("OK")

		case "GET":
			parts := strings.SplitN(line, " ", 2)
			if len(parts) < 2 {
				writeLine("ERR syntax: GET <key>")
				continue
			}
			val, exists := s.db.Get(strings.TrimSpace(parts[1]))
			if !exists {
				writeLine("(nil)")
			} else {
				writeLine(val)
			}

		case "DEL":
			parts := strings.SplitN(line, " ", 2)
			if len(parts) < 2 {
				writeLine("ERR syntax: DEL <key>")
				continue
			}
			deleted := s.db.Delete(strings.TrimSpace(parts[1]))
			if deleted {
				s.logCommand(line)
				writeLine("1")
			} else {
				writeLine("0")
			}

		case "BGREWRITEAOF":
			if s.aof == nil {
				writeLine("ERR AOF not configured")
				continue
			}
			if !s.rewriting.CompareAndSwap(false, true) {
				writeLine("ERR rewrite already in progress")
				continue
			}
			go func() {
				defer s.rewriting.Store(false)
				if err := s.aof.Rewrite(s.db.SnapshotCommands); err != nil {
					log.Printf("BGREWRITEAOF error: %v", err)
				}
			}()
			writeLine("Background append only file rewriting started")

		case "SUBSCRIBE":
			if s.pubsub == nil {
				writeLine("ERR Pub/Sub not configured")
				continue
			}
			parts := strings.SplitN(line, " ", 2)
			if len(parts) < 2 || strings.TrimSpace(parts[1]) == "" {
				writeLine("ERR syntax: SUBSCRIBE <channel>")
				continue
			}
			channel := strings.TrimSpace(parts[1])
			if _, exists := subs[channel]; !exists {
				msgCh, unsub := s.pubsub.Subscribe(channel)
				subs[channel] = unsub
				wg.Add(1)
				go func(chName string, ch <-chan string) {
					defer wg.Done()
					for msg := range ch {
						writeLine(fmt.Sprintf("MESSAGE %s %s", chName, msg))
					}
				}(channel, msgCh)
			}
			writeLine("SUBSCRIBED " + channel)

		case "UNSUBSCRIBE":
			if s.pubsub == nil {
				writeLine("ERR Pub/Sub not configured")
				continue
			}
			parts := strings.SplitN(line, " ", 2)
			if len(parts) < 2 || strings.TrimSpace(parts[1]) == "" {
				writeLine("ERR syntax: UNSUBSCRIBE <channel>")
				continue
			}
			channel := strings.TrimSpace(parts[1])
			if unsub, exists := subs[channel]; exists {
				unsub()
				delete(subs, channel)
			}
			writeLine("UNSUBSCRIBED " + channel)

		case "PUBLISH":
			if s.pubsub == nil {
				writeLine("ERR Pub/Sub not configured")
				continue
			}
			parts := strings.SplitN(line, " ", 3)
			if len(parts) < 3 {
				writeLine("ERR syntax: PUBLISH <channel> <message>")
				continue
			}
			count := s.pubsub.Publish(parts[1], parts[2])
			writeLine(strconv.Itoa(count))

		default:
			writeLine("ERR unknown command")
		}
	}
}

func (s *Server) logCommand(line string) {
	if s.aof == nil {
		return
	}
	if err := s.aof.Append(line); err != nil {
		log.Printf("Failed to append command to AOF: %v", err)
	}
}