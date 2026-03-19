package simulator

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

type SeqLogger struct {
	mu sync.Mutex
	f  *os.File
}

func NewSeqLogger(path string) (*SeqLogger, error) {
	if path == "" {
		return &SeqLogger{}, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &SeqLogger{f: f}, nil
}

func (l *SeqLogger) Write(entry map[string]any) {
	if l == nil || l.f == nil {
		return
	}
	line, err := json.Marshal(entry)
	if err != nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	_, _ = l.f.Write(append(line, '\n'))
}

func (l *SeqLogger) Close() error {
	if l == nil || l.f == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.f.Close()
}
