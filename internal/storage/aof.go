package storage

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"
)

type AOF struct {
	file *os.File
	mu   sync.Mutex
}

func NewAOF(path string) (*AOF, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	return &AOF{file: file}, nil
}

// grava o comando no arquivo
func (aof *AOF) Append(command string) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()
	_, err := aof.file.WriteString(command + "\n")
	return err
}

// replay - tpio, lê o arquivo no boot do servidor e recria o estado da memória
func (aof *AOF) Replay(db *DB) error {
	aof.file.Seek(0, 0)
	scanner := bufio.NewScanner(aof.file)

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, " ", 4)
		if len(parts) == 0 {
			continue
		}
		
		cmd := strings.ToUpper(parts[0])
		if cmd == "SET" && len(parts) == 3 {
			db.Set(parts[1], parts[2], 0)
		} else if cmd == "SETEX" && len(parts) == 4 {
			ttl, _ := strconv.Atoi(parts[2])
			db.Set(parts[1], parts[3], ttl)
		} else if cmd == "DEL" && len(parts) == 2 {
			db.Delete(parts[1])
		}
	}
	return scanner.Err()
}

func (aof *AOF) Close() error {
	return aof.file.Close()
}