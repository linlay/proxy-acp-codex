package platform

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSSEWriterWritesMessageAndDone(t *testing.T) {
	rec := httptest.NewRecorder()
	writer, err := NewSSEWriter(rec)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	if err := writer.Publish(NewEvent(1, "content.delta", map[string]any{"contentId": "c1", "delta": "hi"})); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := writer.WriteDone(); err != nil {
		t.Fatalf("done: %v", err)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "event: message\n") || !strings.Contains(body, `"type":"content.delta"`) || !strings.Contains(body, "data: [DONE]") {
		t.Fatalf("unexpected body %q", body)
	}
}
