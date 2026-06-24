package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	"proxy-acp-codex/internal/config"
	"proxy-acp-codex/internal/platform"
)

type codexDebugModelsPayload struct {
	Models []codexDebugModel `json:"models"`
}

type codexDebugModel struct {
	Slug                     string                  `json:"slug"`
	DisplayName              string                  `json:"display_name"`
	ContextWindow            int                     `json:"context_window"`
	SupportedReasoningLevels []codexReasoningLevel   `json:"supported_reasoning_levels"`
	AdditionalSpeedTiers     []string                `json:"additional_speed_tiers"`
	ServiceTiers             []codexDebugServiceTier `json:"service_tiers"`
	Visibility               string                  `json:"visibility"`
}

type codexReasoningLevel struct {
	Effort string `json:"effort"`
}

type codexDebugServiceTier struct {
	ID string `json:"id"`
}

func (s *Server) handleModels(w http.ResponseWriter, r *http.Request) {
	resp, err := s.queryCodexModels(r.Context())
	if err != nil {
		platform.WriteJSON(w, http.StatusBadGateway, platform.Failure(http.StatusBadGateway, err.Error()))
		return
	}
	platform.WriteJSON(w, http.StatusOK, platform.Success(resp))
}

func (s *Server) queryCodexModels(ctx context.Context) (platform.ModelCatalogResponse, error) {
	command, env, err := s.codexCLICommand()
	if err != nil {
		return platform.ModelCatalogResponse{}, err
	}
	runCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	cmd := exec.CommandContext(runCtx, command, "debug", "models")
	cmd.Env = append(os.Environ(), env...)
	output, err := cmd.Output()
	if err != nil {
		if runCtx.Err() != nil {
			return platform.ModelCatalogResponse{}, fmt.Errorf("query codex models: %w", runCtx.Err())
		}
		if exitErr, ok := err.(*exec.ExitError); ok {
			stderr := strings.TrimSpace(string(exitErr.Stderr))
			if stderr != "" {
				return platform.ModelCatalogResponse{}, fmt.Errorf("query codex models: %s", stderr)
			}
		}
		return platform.ModelCatalogResponse{}, fmt.Errorf("query codex models: %w", err)
	}

	var decoded codexDebugModelsPayload
	if err := json.Unmarshal(output, &decoded); err != nil {
		return platform.ModelCatalogResponse{}, fmt.Errorf("decode codex models: %w", err)
	}

	items := make([]platform.ModelCatalogItem, 0, len(decoded.Models))
	for _, model := range decoded.Models {
		if !shouldListCodexModel(model) {
			continue
		}
		items = append(items, platform.ModelCatalogItem{
			Key:           strings.TrimSpace(model.Slug),
			Name:          strings.TrimSpace(model.DisplayName),
			ModelID:       strings.TrimSpace(model.Slug),
			ContextWindow: model.ContextWindow,
			IsReasoner:    len(model.SupportedReasoningLevels) > 0,
			ServiceTiers:  codexModelServiceTiers(model),
		})
	}
	return platform.ModelCatalogResponse{Models: items}, nil
}

func (s *Server) codexCLICommand() (string, []string, error) {
	backend, ok := s.cfg.Backend(s.cfg.DefaultBackend)
	if !ok {
		return "", nil, fmt.Errorf("default backend %q is not configured", s.cfg.DefaultBackend)
	}
	if backend.Command != config.SelfBackendCommand {
		return "", nil, fmt.Errorf("model discovery is only supported for the built-in codex backend")
	}
	command := "codex"
	for idx := 0; idx < len(backend.Args)-1; idx++ {
		if backend.Args[idx] == "-codex" && strings.TrimSpace(backend.Args[idx+1]) != "" {
			command = strings.TrimSpace(backend.Args[idx+1])
			break
		}
	}
	env := make([]string, 0, len(backend.Env))
	for key, value := range backend.Env {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		env = append(env, key+"="+value)
	}
	return command, env, nil
}

func shouldListCodexModel(model codexDebugModel) bool {
	if strings.TrimSpace(model.Slug) == "" {
		return false
	}
	visibility := strings.ToLower(strings.TrimSpace(model.Visibility))
	return visibility == "" || visibility == "list"
}

func codexModelServiceTiers(model codexDebugModel) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, 2)
	appendTier := func(raw string) {
		tier := normalizeCodexModelServiceTier(raw)
		if tier == "" {
			return
		}
		if _, ok := seen[tier]; ok {
			return
		}
		seen[tier] = struct{}{}
		out = append(out, tier)
	}
	for _, tier := range model.AdditionalSpeedTiers {
		appendTier(tier)
	}
	for _, tier := range model.ServiceTiers {
		appendTier(tier.ID)
	}
	return out
}

func normalizeCodexModelServiceTier(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "priority", "fast":
		return "FAST"
	case "flex":
		return "FLEX"
	default:
		return ""
	}
}
