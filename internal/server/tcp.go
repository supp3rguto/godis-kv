package server

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/supp3rguto/godis-kv/internal/storage"
)

type Server struct {
	addr     string
	db       *storage.DB
	aof      *storage.AOF
	listener net.Listener
}

func NewServer(addr string, db *storage.DB, aof *storage.AOF) *Server {
	return &Server{addr: addr, db: db, aof: aof}
}

func (s *Server) Start() error {
	var err error
	s.listener, err = net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	fmt.Printf("Servidor TCP rodando na porta %s\n", s.addr)

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			// Se o erro for de fechamento do listener (Graceful Shutdown), apenas sai do loop
			if strings.Contains(err.Error(), "use of closed network connection") {
				break
			}
			continue
		}
		go s.handleConnection(conn)
	}
	return nil
}

// Função para ajudar no desligamento elegante
func (s *Server) Stop() {
	if s.listener != nil {
		s.listener.Close()
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		line := scanner.Text()
		// Permite dividir em até 4 partes para suportar SETEX chave seg valor
		parts := strings.SplitN(strings.TrimSpace(line), " ", 4)
		if len(parts) == 0 || parts[0] == "" {
			continue
		}

		command := strings.ToUpper(parts[0])

		switch command {
		case "SET":
			if len(parts) < 3 {
				conn.Write([]byte("ERR Sintaxe: SET <chave> <valor>\n"))
				continue
			}
			s.db.Set(parts[1], parts[2], 0) // 0 TTL = permanente
			s.aof.Append(line)
			conn.Write([]byte("OK\n"))

		case "SETEX":
			if len(parts) < 4 {
				conn.Write([]byte("ERR Sintaxe: SETEX <chave> <segundos> <valor>\n"))
				continue
			}
			ttl, err := strconv.Atoi(parts[2])
			if err != nil {
				conn.Write([]byte("ERR Segundos invalidos\n"))
				continue
			}
			s.db.Set(parts[1], parts[3], ttl)
			s.aof.Append(line)
			conn.Write([]byte("OK\n"))

		case "GET":
			if len(parts) < 2 {
				conn.Write([]byte("ERR Sintaxe: GET <chave>\n"))
				continue
			}
			val, exists := s.db.Get(parts[1])
			if !exists {
				conn.Write([]byte("(nil)\n"))
			} else {
				conn.Write([]byte(val + "\n"))
			}

		case "DEL":
			if len(parts) < 2 {
				conn.Write([]byte("ERR Sintaxe: DEL <chave>\n"))
				continue
			}
			deleted := s.db.Delete(parts[1])
			if deleted {
				s.aof.Append(line)
				conn.Write([]byte("1\n"))
			} else {
				conn.Write([]byte("0\n"))
			}

		default:
			conn.Write([]byte("ERR Comando desconhecido\n"))
		}
	}
}