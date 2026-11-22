package storage

import (
	"bufio"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

type AOF struct {
	path   string
	file   *os.File
	closed bool
	mu     sync.Mutex
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
	return &AOF{path: path, file: file}, nil
}

func (aof *AOF) Append(command string) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	if aof.closed {
		return errors.New("aof fechado")
	}

	_, err := aof.file.WriteString(command + "\n")
	return err
}

// Rewrite compacta o log em um arquivo temporário e faz substituição atômica via os.Rename.
func (aof *AOF) Rewrite(snapshotFn func() []string) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	if aof.closed {
		return errors.New("aof fechado")
	}

	commands := snapshotFn()
	tmpPath := aof.path + ".tmp"

	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}

	writer := bufio.NewWriter(tmpFile)
	for _, cmd := range commands {
		if _, err := writer.WriteString(cmd + "\n"); err != nil {
			tmpFile.Close()
			os.Remove(tmpPath)
			return err
		}
	}

	if err := writer.Flush(); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return err
	}

	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return err
	}

	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}

	if err := aof.file.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}

	if err := os.Rename(tmpPath, aof.path); err != nil {
		os.Remove(tmpPath)
		// Tenta reabrir o arquivo original para não deixar o ponteiro fechado
		if reopened, reopenErr := os.OpenFile(aof.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666); reopenErr == nil {
			aof.file = reopened
		} else {
			aof.closed = true
		}
		return err
	}

	newFile, err := os.OpenFile(aof.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		aof.closed = true
		return err
	}

	aof.file = newFile
	return nil
}

func (aof *AOF) Replay(db *DB) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	if aof.closed {
		return errors.New("aof fechado")
	}

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

	if aof.closed {
		return nil
	}
	aof.closed = true
	return aof.file.Close()
}