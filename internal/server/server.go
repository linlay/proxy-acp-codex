package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"proxy-acp-codex/internal/acpbridge"
	"proxy-acp-codex/internal/config"
	"proxy-acp-codex/internal/platform"
)

type Server struct {
	cfg     config.Config
	manager *acpbridge.Manager
	mux     *http.ServeMux
}

func New(cfg config.Config, manager *acpbridge.Manager) http.Handler {
	s := &Server{cfg: cfg, manager: manager, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) routes() {
	s.mux.HandleFunc("/healthz", s.handleHealthz)
	s.mux.HandleFunc("/api/query", s.withAuth(http.MethodPost, s.handleQuery))
	s.mux.HandleFunc("/api/submit", s.withAuth(http.MethodPost, s.handleSubmit))
	s.mux.HandleFunc("/api/steer", s.withAuth(http.MethodPost, s.handleSteer))
	s.mux.HandleFunc("/api/interrupt", s.withAuth(http.MethodPost, s.handleInterrupt))
	s.mux.HandleFunc("/ws", s.handleWS)
}

func (s *Server) withAuth(method string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != method {
			platform.WriteJSON(w, http.StatusMethodNotAllowed, platform.Failure(http.StatusMethodNotAllowed, "method not allowed"))
			return
		}
		if !s.authorized(r) {
			platform.WriteJSON(w, http.StatusUnauthorized, platform.Failure(http.StatusUnauthorized, "unauthorized"))
			return
		}
		next(w, r)
	}
}

func (s *Server) authorized(r *http.Request) bool {
	if strings.TrimSpace(s.cfg.AuthToken) == "" {
		return true
	}
	token := strings.TrimSpace(r.Header.Get("Authorization"))
	if strings.HasPrefix(strings.ToLower(token), "bearer ") {
		token = strings.TrimSpace(token[7:])
	}
	if token == "" {
		token = strings.TrimSpace(r.URL.Query().Get("token"))
	}
	return token == s.cfg.AuthToken
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	platform.WriteJSON(w, http.StatusOK, platform.Success(map[string]any{"ok": true}))
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	var req platform.QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		platform.WriteJSON(w, http.StatusBadRequest, platform.Failure(http.StatusBadRequest, "invalid query payload"))
		return
	}
	normalizeQuery(&req)

	writer, err := platform.NewSSEWriter(w)
	if err != nil {
		platform.WriteJSON(w, http.StatusInternalServerError, platform.Failure(http.StatusInternalServerError, err.Error()))
		return
	}
	defer writer.Close()

	if err := s.manager.Execute(r.Context(), req, writer); err != nil {
		log.Printf("[proxy-acp-codex] query failed run=%s: %v", req.RunID, err)
	}
	_ = writer.WriteDone()
}

func (s *Server) handleSubmit(w http.ResponseWriter, r *http.Request) {
	var req platform.SubmitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.RunID) == "" || strings.TrimSpace(req.AwaitingID) == "" {
		platform.WriteJSON(w, http.StatusBadRequest, platform.Failure(http.StatusBadRequest, "runId and awaitingId are required"))
		return
	}
	resp, _ := s.manager.Submit(req)
	platform.WriteJSON(w, http.StatusOK, platform.Success(resp))
}

func (s *Server) handleSteer(w http.ResponseWriter, r *http.Request) {
	var req platform.SteerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.RunID) == "" || strings.TrimSpace(req.Message) == "" {
		platform.WriteJSON(w, http.StatusBadRequest, platform.Failure(http.StatusBadRequest, "runId and message are required"))
		return
	}
	platform.WriteJSON(w, http.StatusOK, platform.Success(s.manager.Steer(req)))
}

func (s *Server) handleInterrupt(w http.ResponseWriter, r *http.Request) {
	var req platform.InterruptRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.RunID) == "" {
		platform.WriteJSON(w, http.StatusBadRequest, platform.Failure(http.StatusBadRequest, "runId is required"))
		return
	}
	platform.WriteJSON(w, http.StatusOK, platform.Success(s.manager.Interrupt(req)))
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(*http.Request) bool { return true },
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		platform.WriteJSON(w, http.StatusUnauthorized, platform.Failure(http.StatusUnauthorized, "unauthorized"))
		return
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	connCtx, cancel := context.WithCancel(r.Context())
	defer cancel()
	_ = conn.WriteJSON(pushFrame{Frame: "push", Type: "connected", Data: map[string]any{"sessionId": "proxy-acp-codex"}})

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var frame requestFrame
		if err := json.Unmarshal(data, &frame); err != nil {
			_ = conn.WriteJSON(errorFrame{Frame: "error", ID: "", Type: "invalid_request", Code: 400, Msg: "invalid websocket frame"})
			continue
		}
		switch frame.Type {
		case "request.query", "/api/query":
			go s.wsQuery(connCtx, conn, frame)
		case "request.submit", "/api/submit":
			s.wsSubmit(conn, frame)
		case "request.steer", "/api/steer":
			s.wsSteer(conn, frame)
		case "request.interrupt", "/api/interrupt":
			s.wsInterrupt(conn, frame)
		default:
			_ = conn.WriteJSON(errorFrame{Frame: "error", ID: frame.ID, Type: "not_found", Code: 404, Msg: "unsupported request type"})
		}
	}
}

func (s *Server) wsQuery(ctx context.Context, conn *websocket.Conn, frame requestFrame) {
	req, err := platform.DecodeJSON[platform.QueryRequest](frame.Payload)
	if err != nil {
		writeWS(conn, errorFrame{Frame: "error", ID: frame.ID, Type: "invalid_request", Code: 400, Msg: "invalid query payload"})
		return
	}
	normalizeQuery(&req)
	streamID := "s_" + req.RunID
	sink := &wsSink{conn: conn, id: frame.ID, streamID: streamID}
	writeWS(conn, responseFrame{Frame: "response", Type: frame.Type, ID: frame.ID, Code: 0, Msg: "success", Data: map[string]any{"runId": req.RunID, "streamId": streamID}})
	if err := s.manager.Execute(ctx, req, sink); err != nil {
		log.Printf("[proxy-acp-codex][ws] query failed run=%s: %v", req.RunID, err)
	}
	writeWS(conn, streamFrame{Frame: "stream", ID: frame.ID, StreamID: streamID, Reason: "done"})
}

func (s *Server) wsSubmit(conn *websocket.Conn, frame requestFrame) {
	req, err := platform.DecodeJSON[platform.SubmitRequest](frame.Payload)
	if err != nil {
		writeWS(conn, errorFrame{Frame: "error", ID: frame.ID, Type: "invalid_request", Code: 400, Msg: "invalid submit payload"})
		return
	}
	resp, _ := s.manager.Submit(req)
	writeWS(conn, responseFrame{Frame: "response", Type: frame.Type, ID: frame.ID, Code: 0, Msg: "success", Data: resp})
}

func (s *Server) wsSteer(conn *websocket.Conn, frame requestFrame) {
	req, err := platform.DecodeJSON[platform.SteerRequest](frame.Payload)
	if err != nil || strings.TrimSpace(req.RunID) == "" || strings.TrimSpace(req.Message) == "" {
		writeWS(conn, errorFrame{Frame: "error", ID: frame.ID, Type: "invalid_request", Code: 400, Msg: "invalid steer payload"})
		return
	}
	resp := s.manager.Steer(req)
	writeWS(conn, responseFrame{Frame: "response", Type: frame.Type, ID: frame.ID, Code: 0, Msg: "success", Data: resp})
}

func (s *Server) wsInterrupt(conn *websocket.Conn, frame requestFrame) {
	req, err := platform.DecodeJSON[platform.InterruptRequest](frame.Payload)
	if err != nil {
		writeWS(conn, errorFrame{Frame: "error", ID: frame.ID, Type: "invalid_request", Code: 400, Msg: "invalid interrupt payload"})
		return
	}
	resp := s.manager.Interrupt(req)
	writeWS(conn, responseFrame{Frame: "response", Type: frame.Type, ID: frame.ID, Code: 0, Msg: "success", Data: resp})
}

type requestFrame struct {
	Frame   string          `json:"frame"`
	Type    string          `json:"type"`
	ID      string          `json:"id"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

type responseFrame struct {
	Frame string `json:"frame"`
	Type  string `json:"type"`
	ID    string `json:"id"`
	Code  int    `json:"code"`
	Msg   string `json:"msg"`
	Data  any    `json:"data,omitempty"`
}

type streamFrame struct {
	Frame    string              `json:"frame"`
	ID       string              `json:"id"`
	StreamID string              `json:"streamId"`
	Event    *platform.EventData `json:"event,omitempty"`
	Reason   string              `json:"reason,omitempty"`
}

type pushFrame struct {
	Frame string `json:"frame"`
	Type  string `json:"type"`
	Data  any    `json:"data,omitempty"`
}

type errorFrame struct {
	Frame string `json:"frame"`
	Type  string `json:"type"`
	ID    string `json:"id,omitempty"`
	Code  int    `json:"code"`
	Msg   string `json:"msg"`
	Data  any    `json:"data,omitempty"`
}

type wsSink struct {
	conn     *websocket.Conn
	id       string
	streamID string
}

func (s *wsSink) Publish(event platform.EventData) error {
	return writeWS(s.conn, streamFrame{Frame: "stream", ID: s.id, StreamID: s.streamID, Event: &event})
}

var wsWriteMu sync.Mutex

func writeWS(conn *websocket.Conn, value any) error {
	wsWriteMu.Lock()
	defer wsWriteMu.Unlock()
	return conn.WriteJSON(value)
}

func normalizeQuery(req *platform.QueryRequest) {
	if strings.TrimSpace(req.RequestID) == "" {
		req.RequestID = "req_" + randomish()
	}
	if strings.TrimSpace(req.RunID) == "" {
		req.RunID = "run_" + randomish()
	}
	if strings.TrimSpace(req.ChatID) == "" {
		req.ChatID = "chat_" + req.RunID
	}
	if strings.TrimSpace(req.Role) == "" {
		req.Role = "user"
	}
}

func randomish() string {
	return fmt.Sprintf("%x", time.Now().UnixNano())
}
