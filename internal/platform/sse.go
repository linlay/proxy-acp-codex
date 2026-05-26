package platform

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
)

type SSEWriter struct {
	w       http.ResponseWriter
	flusher http.Flusher
	mu      sync.Mutex
	closed  bool
}

func NewSSEWriter(w http.ResponseWriter) (*SSEWriter, error) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return nil, fmt.Errorf("streaming not supported")
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	flusher.Flush()
	return &SSEWriter{w: w, flusher: flusher}, nil
}

func (s *SSEWriter) Publish(event EventData) error {
	return s.WriteJSON("message", event)
}

func (s *SSEWriter) WriteJSON(eventName string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return s.writeRaw(fmt.Sprintf("event: %s\ndata: %s\n\n", eventName, data))
}

func (s *SSEWriter) WriteDone() error {
	return s.writeRaw("event: message\ndata: " + DoneSentinel + "\n\n")
}

func (s *SSEWriter) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return nil
}

func (s *SSEWriter) writeRaw(raw string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("sse writer closed")
	}
	if _, err := fmt.Fprint(s.w, raw); err != nil {
		return err
	}
	s.flusher.Flush()
	return nil
}
