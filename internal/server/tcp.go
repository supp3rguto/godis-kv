package server

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
)

// KeyValueStore define o contrato de armazenamento em memória consumido pelo servidor.
type KeyValueStore interface {
	Set(key, value string, ttlSeconds int)
	Get(key string) (string, bool)
	Delete(key string) bool
	SnapshotCommands() []string
}

// AppendLogger define o contrato de persistência de comandos em disco.
type AppendLogger interface {
	Append(command string) error
	Rewrite(snapshotFn func() []string) error
}

type Server struct {
	addr      string
	db        KeyValueStore
	aof       AppendLogger
	listener  net.Listener
	rewriting atomic.Bool
}

func NewServer(addr string, db KeyValueStore, aof AppendLogger) *Server {
	return &Server{
		addr: addr,
		db:   db,
		aof:  aof,
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
	defer conn.Close()
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
				conn.Write([]byte("ERR Sintaxe: SET <chave> <valor>\n"))
				continue
			}
			s.db.Set(parts[1], parts[2], 0)
			s.logCommand(line)
			conn.Write([]byte("OK\n"))

		case "SETEX":
			parts := strings.SplitN(line, " ", 4)
			if len(parts) < 4 {
				conn.Write([]byte("ERR Sintaxe: SETEX <chave> <segundos> <valor>\n"))
				continue
			}
			ttl, err := strconv.Atoi(parts[2])
			if err != nil || ttl <= 0 {
				conn.Write([]byte("ERR Segundos invalidos\n"))
				continue
			}
			s.db.Set(parts[1], parts[3], ttl)
			s.logCommand(line)
			conn.Write([]byte("OK\n"))

		case "GET":
			parts := strings.SplitN(line, " ", 2)
			if len(parts) < 2 {
				conn.Write([]byte("ERR Sintaxe: GET <chave>\n"))
				continue
			}
			val, exists := s.db.Get(strings.TrimSpace(parts[1]))
			if !exists {
				conn.Write([]byte("(nil)\n"))
			} else {
				conn.Write([]byte(val + "\n"))
			}

		case "DEL":
			parts := strings.SplitN(line, " ", 2)
			if len(parts) < 2 {
				conn.Write([]byte("ERR Sintaxe: DEL <chave>\n"))
				continue
			}
			deleted := s.db.Delete(strings.TrimSpace(parts[1]))
			if deleted {
				s.logCommand(line)
				conn.Write([]byte("1\n"))
			} else {
				conn.Write([]byte("0\n"))
			}

		case "BGREWRITEAOF":
			if s.aof == nil {
				conn.Write([]byte("ERR AOF nao configurado\n"))
				continue
			}
			if !s.rewriting.CompareAndSwap(false, true) {
				conn.Write([]byte("ERR Rewrite ja em andamento\n"))
				continue
			}
			go func() {
				defer s.rewriting.Store(false)
				if err := s.aof.Rewrite(s.db.SnapshotCommands); err != nil {
					log.Printf("Erro no BGREWRITEAOF: %v", err)
				}
			}()
			conn.Write([]byte("Background append only file rewriting started\n"))

		default:
			conn.Write([]byte("ERR Comando desconhecido\n"))
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