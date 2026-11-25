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

// define o contrato de armazenamento em memória consumido pelo servidor
type KeyValueStore interface {
	Set(key, value string, ttlSeconds int)
	Get(key string) (string, bool)
	Delete(key string) bool
	SnapshotCommands() []string
}

//define o contrato de persistência de comandos em disco
type AppendLogger interface {
	Append(command string) error
	Rewrite(snapshotFn func() []string) error
}

// define o contrato de mensageria em tempo real
type PubSubBroker interface {
	Subscribe(channel string) (<-chan string, func())
	Publish(channel, message string) int
}

type Server struct {
	addr      string
	db        KeyValueStore
	aof       AppendLogger
	pubsub    PubSubBroker
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
	var err error
	s.listener, err = net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	fmt.Printf("Servidor TCP rodando em %s\n", s.addr)

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				break
			}
			log.Printf("Erro ao aceitar conexão: %v", err)
			continue
		}
		go s.handleConnection(conn)
	}
	return nil
}

func (s *Server) Stop() {
	if s.listener != nil {
		s.listener.Close()
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	var (
		writeMu sync.Mutex
		wg      sync.WaitGroup
		subs    = make(map[string]func())
	)

	// ordem LIFO no encerramento:
	// 1-cancela inscricoes (fecha os go channels)
	// 2-fecha o socket tcp (desbloqueia qualquer escrita pendente)
	// 3-aguarda todas as goroutines de entrega finalizarem
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
				writeLine("ERR Sintaxe: SET <chave> <valor>")
				continue
			}
			s.db.Set(parts[1], parts[2], 0)
			s.logCommand(line)
			writeLine("OK")

		case "SETEX":
			parts := strings.SplitN(line, " ", 4)
			if len(parts) < 4 {
				writeLine("ERR Sintaxe: SETEX <chave> <segundos> <valor>")
				continue
			}
			ttl, err := strconv.Atoi(parts[2])
			if err != nil || ttl <= 0 {
				writeLine("ERR Segundos invalidos")
				continue
			}
			s.db.Set(parts[1], parts[3], ttl)
			s.logCommand(line)
			writeLine("OK")

		case "GET":
			parts := strings.SplitN(line, " ", 2)
			if len(parts) < 2 {
				writeLine("ERR Sintaxe: GET <chave>")
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
				writeLine("ERR Sintaxe: DEL <chave>")
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
				writeLine("ERR AOF nao configurado")
				continue
			}
			if !s.rewriting.CompareAndSwap(false, true) {
				writeLine("ERR Rewrite ja em andamento")
				continue
			}
			go func() {
				defer s.rewriting.Store(false)
				if err := s.aof.Rewrite(s.db.SnapshotCommands); err != nil {
					log.Printf("Erro no BGREWRITEAOF: %v", err)
				}
			}()
			writeLine("Background append only file rewriting started")

		case "SUBSCRIBE":
			if s.pubsub == nil {
				writeLine("ERR Pub/Sub nao configurado")
				continue
			}
			parts := strings.SplitN(line, " ", 2)
			if len(parts) < 2 || strings.TrimSpace(parts[1]) == "" {
				writeLine("ERR Sintaxe: SUBSCRIBE <canal>")
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
				writeLine("ERR Pub/Sub nao configurado")
				continue
			}
			parts := strings.SplitN(line, " ", 2)
			if len(parts) < 2 || strings.TrimSpace(parts[1]) == "" {
				writeLine("ERR Sintaxe: UNSUBSCRIBE <canal>")
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
				writeLine("ERR Pub/Sub nao configurado")
				continue
			}
			parts := strings.SplitN(line, " ", 3)
			if len(parts) < 3 {
				writeLine("ERR Sintaxe: PUBLISH <canal> <mensagem>")
				continue
			}
			count := s.pubsub.Publish(parts[1], parts[2])
			writeLine(strconv.Itoa(count))

		default:
			writeLine("ERR Comando desconhecido")
		}
	}
}

func (s *Server) logCommand(line string) {
	if s.aof == nil {
		return
	}
	if err := s.aof.Append(line); err != nil {
		log.Printf("Erro ao persistir comando no AOF: %v", err)
	}
}