package storage

import (
	"bufio"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

type AOF struct {
	file *os.File
	mu   sync.Mutex
}

func NewAOF(path string) (*AOF, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	return &AOF{file: file}, nil
}

func (aof *AOF) Append(command string) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()
	_, err := aof.file.WriteString(command + "\n")
	return err
}

func (aof *AOF) Replay(db *DB) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	if _, err := aof.file.Seek(0, 0); err != nil {
		return err
	}

	scanner := bufio.NewScanner(aof.file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		cmdParts := strings.SplitN(line, " ", 2)
		cmd := strings.ToUpper(cmdParts[0])

		switch cmd {
		case "SET":
			parts := strings.SplitN(line, " ", 3)
			if len(parts) == 3 {
				db.Set(parts[1], parts[2], 0)
			}
		case "SETEX":
			parts := strings.SplitN(line, " ", 4)
			if len(parts) == 4 {
				if ttl, err := strconv.Atoi(parts[2]); err == nil {
					db.Set(parts[1], parts[3], ttl)
				}
			}
		case "DEL":
			parts := strings.SplitN(line, " ", 2)
			if len(parts) == 2 {
				db.Delete(strings.TrimSpace(parts[1]))
			}
		}
	}
	return scanner.Err()
}

func (aof *AOF) Close() error {
	aof.mu.Lock()
	defer aof.mu.Unlock()
	return aof.file.Close()
}