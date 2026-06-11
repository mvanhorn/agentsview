package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/wesm/agentsview/internal/db"
	"github.com/wesm/agentsview/internal/insight"
	"github.com/wesm/agentsview/internal/timeutil"
)

var validInsightTypes = map[string]bool{
	"daily_activity":   true,
	"agent_analysis":   true,
	insight.CannedType: true,
}

func (s *Server) handleListInsights(
	w http.ResponseWriter, r *http.Request,
) {
	q := r.URL.Query()

	typ := q.Get("type")
	if typ != "" && !validInsightTypes[typ] {
		writeError(w, http.StatusBadRequest,
			"invalid type: must be daily_activity, agent_analysis, or llm_canned")
		return
	}

	filter := db.InsightFilter{
		Type:    typ,
		Project: q.Get("project"),
	}

	insights, err := s.db.ListInsights(
		r.Context(), filter,
	)
	if err != nil {
		if handleContextError(w, err) {
			return
		}
		writeError(
			w, http.StatusInternalServerError, err.Error(),
		)
		return
	}
	if insights == nil {
		insights = []db.Insight{}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"insights": insights,
	})
}

func (s *Server) handleGetInsight(
	w http.ResponseWriter, r *http.Request,
) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}

	result, err := s.db.GetInsight(r.Context(), id)
	if err != nil {
		if handleContextError(w, err) {
			return
		}
		writeError(
			w, http.StatusInternalServerError, err.Error(),
		)
		return
	}
	if result == nil {
		writeError(w, http.StatusNotFound, "insight not found")
		return
	}

	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleDeleteInsight(
	w http.ResponseWriter, r *http.Request,
) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}

	existing, err := s.db.GetInsight(r.Context(), id)
	if err != nil {
		if handleContextError(w, err) {
			return
		}
		writeError(
			w, http.StatusInternalServerError, err.Error(),
		)
		return
	}
	if existing == nil {
		writeError(w, http.StatusNotFound, "insight not found")
		return
	}

	if err := s.db.DeleteInsight(id); err != nil {
		if handleReadOnly(w, err) {
			return
		}
		writeError(
			w, http.StatusInternalServerError, err.Error(),
		)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

type generateInsightRequest struct {
	Type           string `json:"type"`
	DateFrom       string `json:"date_from"`
	DateTo         string `json:"date_to"`
	Project        string `json:"project"`
	Prompt         string `json:"prompt"`
	Agent          string `json:"agent"`
	Kind           string `json:"kind"`
	LLMOptIn       bool   `json:"llm_opt_in"`
	ForceRefresh   bool   `json:"force_refresh"`
	AutomatedScope string `json:"automated_scope"`
}

func normalizeInsightAutomatedScope(scope string) (string, bool) {
	switch strings.TrimSpace(scope) {
	case "", "human":
		return "human", true
	case "all", "automated":
		return strings.TrimSpace(scope), true
	default:
		return "", false
	}
}

func insightGenerateClientMessage(
	agent string, err error,
) string {
	if err == nil {
		return fmt.Sprintf("%s generation failed", agent)
	}
	msg := err.Error()
	// Strip stderr dump after newline for the short
	// client message; full details are in the log stream.
	if idx := strings.Index(msg, "\nstderr:"); idx > 0 {
		msg = msg[:idx]
	}
	if idx := strings.Index(msg, "\nraw:"); idx > 0 {
		msg = msg[:idx]
	}
	return msg
}

func (s *Server) handleGenerateInsight(
	w http.ResponseWriter, r *http.Request,
) {
	if s.db.ReadOnly() {
		writeError(w, http.StatusNotImplemented,
			"insight generation is not available in read-only mode")
		return
	}

	var req generateInsightRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest,
			"invalid JSON body")
		return
	}
	scope, ok := normalizeInsightAutomatedScope(req.AutomatedScope)
	if !ok {
		writeError(w, http.StatusBadRequest,
			"automated_scope must be human, all, or automated")
		return
	}
	req.AutomatedScope = scope

	if req.Kind != "" {
		s.handleGenerateCannedInsight(w, r, req)
		return
	}

	if req.Type != "daily_activity" && req.Type != "agent_analysis" {
		writeError(w, http.StatusBadRequest,
			"invalid type: must be daily_activity or agent_analysis")
		return
	}
	if !timeutil.IsValidDate(req.DateFrom) {
		writeError(w, http.StatusBadRequest,
			"invalid date_from: use YYYY-MM-DD")
		return
	}
	if !timeutil.IsValidDate(req.DateTo) {
		writeError(w, http.StatusBadRequest,
			"invalid date_to: use YYYY-MM-DD")
		return
	}
	if req.DateTo < req.DateFrom {
		writeError(w, http.StatusBadRequest,
			"date_to must be >= date_from")
		return
	}

	if req.Agent == "" {
		req.Agent = "claude"
	}
	if !insight.ValidAgents[req.Agent] {
		writeError(w, http.StatusBadRequest,
			"invalid agent: must be one of "+
				strings.Join(insight.ValidAgentNames, ", "))
		return
	}

	stream, err := NewSSEStream(w)
	if err != nil {
		writeError(w, http.StatusInternalServerError,
			"streaming not supported")
		return
	}

	var streamMu sync.Mutex
	sendJSON := func(event string, v any) bool {
		streamMu.Lock()
		defer streamMu.Unlock()
		return stream.SendJSON(event, v)
	}

	if !sendJSON("status", map[string]string{
		"phase": "generating",
	}) {
		return
	}

	prompt, err := insight.BuildPrompt(
		r.Context(), s.db, insight.GenerateRequest{
			Type:           req.Type,
			DateFrom:       req.DateFrom,
			DateTo:         req.DateTo,
			Project:        req.Project,
			Prompt:         req.Prompt,
			AutomatedScope: req.AutomatedScope,
		},
	)
	if err != nil {
		log.Printf("insight prompt error: %v", err)
		sendJSON("error", map[string]string{
			"message": "failed to build prompt",
		})
		return
	}

	genCtx, cancel := context.WithTimeout(
		r.Context(), 10*time.Minute,
	)
	defer cancel()

	const (
		maxBufferedLogEvents = 256
		logDrainTimeout      = 2 * time.Second
		logStopWaitTimeout   = 500 * time.Millisecond
	)
	logCh := make(chan insight.LogEvent, maxBufferedLogEvents)
	logDone := make(chan struct{})
	logStop := make(chan struct{})
	var logStopOnce sync.Once
	stopLogSender := func() {
		logStopOnce.Do(func() { close(logStop) })
	}
	go func() {
		defer close(logDone)
		for {
			select {
			case <-logStop:
				return
			default:
			}
			select {
			case <-logStop:
				return
			case ev, ok := <-logCh:
				if !ok {
					return
				}
				if !sendJSON("log", ev) {
					stopLogSender()
					return
				}
			}
		}
	}()
	var (
		logStateMu    sync.Mutex
		logStreamDone bool
		droppedLogs   int
	)
	enqueueLog := func(ev insight.LogEvent) {
		logStateMu.Lock()
		defer logStateMu.Unlock()
		if logStreamDone {
			return
		}
		select {
		case logCh <- ev:
		default:
			droppedLogs++
		}
	}
	finishLogStream := func() (dropped int, drained bool, senderStopped bool, timedOut bool) {
		logStateMu.Lock()
		logStreamDone = true
		close(logCh)
		dropped = droppedLogs
		logStateMu.Unlock()
		select {
		case <-logDone:
			return dropped, true, true, false
		case <-time.After(logDrainTimeout):
			log.Printf(
				"insight log stream drain timed out after %s",
				logDrainTimeout,
			)
			// Count remaining buffered events as dropped since they will
			// not be delivered once we abort the stream.
			dropped += len(logCh)
			stopLogSender()
			select {
			case <-logDone:
				return dropped, false, true, true
			case <-time.After(logStopWaitTimeout):
				log.Printf(
					"insight log sender stop timed out after %s",
					logStopWaitTimeout,
				)
				// Try to force-unblock any in-flight writer and wait one
				// more bounded interval for sender shutdown.
				stream.ForceWriteDeadlineNow()
				select {
				case <-logDone:
					return dropped, false, true, true
				case <-time.After(logStopWaitTimeout):
					log.Printf(
						"insight log sender did not stop after forced deadline",
					)
					return dropped, false, false, true
				}
			}
		}
	}

	result, err := s.generateStreamFunc(
		genCtx, req.Agent, prompt,
		enqueueLog,
	)
	dropped, drained, senderStopped, timedOut := finishLogStream()
	if !senderStopped {
		stream.ForceWriteDeadlineNow()
		log.Printf("insight log stream sender did not stop; aborting terminal SSE events")
		return
	}
	if dropped > 0 {
		suffix := "due to slow client"
		if timedOut {
			suffix = "due to slow client and log stream timeout"
		}
		sendJSON("log", insight.LogEvent{
			Stream: "stderr",
			Line: fmt.Sprintf(
				"dropped %d log line(s) %s", dropped, suffix,
			),
		})
	}
	if timedOut || !drained {
		log.Printf("insight log stream did not fully drain before completion")
		sendJSON("error", map[string]string{
			"message": "insight log stream timed out before completion",
		})
		return
	}
	if err != nil {
		log.Printf("insight generate error: %v", err)
		sendJSON("error", map[string]string{
			"message": insightGenerateClientMessage(
				req.Agent, err,
			),
		})
		return
	}

	if strings.TrimSpace(result.Content) == "" {
		sendJSON("error", map[string]string{
			"message": "agent returned empty content",
		})
		return
	}

	var project *string
	if req.Project != "" {
		project = &req.Project
	}
	var model *string
	if result.Model != "" {
		model = &result.Model
	}
	var promptPtr *string
	if req.Prompt != "" {
		promptPtr = &req.Prompt
	}

	id, err := s.db.InsertInsight(db.Insight{
		Type:     req.Type,
		DateFrom: req.DateFrom,
		DateTo:   req.DateTo,
		Project:  project,
		Agent:    result.Agent,
		Model:    model,
		Prompt:   promptPtr,
		Content:  result.Content,
	})
	if err != nil {
		log.Printf("insight insert error: %v", err)
		sendJSON("error", map[string]string{
			"message": "failed to save insight",
		})
		return
	}

	saved, err := s.db.GetInsight(r.Context(), id)
	if err != nil || saved == nil {
		log.Printf("insight get error: id=%d err=%v",
			id, err)
		sendJSON("error", map[string]string{
			"message": "failed to retrieve saved insight",
		})
		return
	}

	sendJSON("done", saved)
}

func (s *Server) handleGenerateCannedInsight(
	w http.ResponseWriter,
	r *http.Request,
	req generateInsightRequest,
) {
	req.Prompt = strings.TrimSpace(req.Prompt)
	kind := insight.CannedKind(req.Kind)
	if !insight.ValidCannedKinds[kind] {
		writeError(w, http.StatusBadRequest,
			"invalid kind: unsupported canned insight")
		return
	}
	if req.Type != "" && req.Type != insight.CannedType {
		writeError(w, http.StatusBadRequest,
			"type must be llm_canned for canned insights")
		return
	}
	if !req.LLMOptIn {
		writeError(w, http.StatusBadRequest,
			"llm_opt_in must be true for canned insights")
		return
	}
	if len([]rune(req.Prompt)) > insight.MaxCannedFocusRunes {
		writeError(w, http.StatusBadRequest,
			"prompt is too long for canned insight focus")
		return
	}
	if !timeutil.IsValidDate(req.DateFrom) {
		writeError(w, http.StatusBadRequest,
			"invalid date_from: use YYYY-MM-DD")
		return
	}
	if !timeutil.IsValidDate(req.DateTo) {
		writeError(w, http.StatusBadRequest,
			"invalid date_to: use YYYY-MM-DD")
		return
	}
	if req.DateTo < req.DateFrom {
		writeError(w, http.StatusBadRequest,
			"date_to must be >= date_from")
		return
	}
	if req.Agent == "" {
		req.Agent = "claude"
	}
	if !insight.ValidAgents[req.Agent] {
		writeError(w, http.StatusBadRequest,
			"invalid agent: must be one of "+
				strings.Join(insight.ValidAgentNames, ", "))
		return
	}

	stream, err := NewSSEStream(w)
	if err != nil {
		writeError(w, http.StatusInternalServerError,
			"streaming not supported")
		return
	}
	sendJSON := func(event string, v any) bool {
		return stream.SendJSON(event, v)
	}
	status := func(phase string) bool {
		return sendJSON("status", map[string]string{
			"phase": phase,
		})
	}

	if !status("building_payload") {
		return
	}
	payload, aggregateHash, cacheKey, err := s.buildCannedPayload(
		r.Context(), kind, req,
	)
	if err != nil {
		log.Printf("canned insight payload error: %v", err)
		sendJSON("error", map[string]string{
			"message": "failed to build canned insight payload",
		})
		return
	}

	if !req.ForceRefresh {
		cached, err := s.db.GetCachedInsight(r.Context(), cacheKey)
		if err != nil {
			log.Printf("canned insight cache lookup error: %v", err)
			sendJSON("error", map[string]string{
				"message": "failed to check insight cache",
			})
			return
		}
		if cached != nil {
			markInsightCacheHit(cached)
			if !status("cache_hit") {
				return
			}
			sendJSON("done", cached)
			return
		}
	}

	prompt, err := insight.BuildCannedPrompt(payload, aggregateHash)
	if err != nil {
		log.Printf("canned insight prompt error: %v", err)
		sendJSON("error", map[string]string{
			"message": "failed to build canned prompt",
		})
		return
	}

	if !status("generating") {
		return
	}
	genCtx, cancel := context.WithTimeout(
		r.Context(), 3*time.Minute,
	)
	defer cancel()
	result, err := s.generateStreamFunc(
		genCtx, req.Agent, prompt, nil,
	)
	if err != nil {
		log.Printf("canned insight generate error: %v", err)
		sendJSON("error", map[string]string{
			"message": insightGenerateClientMessage(
				req.Agent, err,
			),
		})
		return
	}

	if !status("validating") {
		return
	}
	envelope, err := insight.ParseCannedEnvelope(result.Content)
	if err != nil {
		log.Printf("canned insight parse error: %v", err)
		sendJSON("error", map[string]string{
			"message": "generated insight was not valid JSON",
		})
		return
	}
	if err := insight.ValidateCannedEnvelope(envelope, payload); err != nil {
		log.Printf("canned insight validation error: %v", err)
		sendJSON("error", map[string]string{
			"message": fmt.Sprintf(
				"generated insight failed validation: %v", err,
			),
		})
		return
	}

	if !status("saving") {
		return
	}
	model := result.Model
	prov, err := insight.NewCannedProvenance(
		payload, aggregateHash, cacheKey, "fresh",
		result.Agent, model, time.Now(),
	)
	if err != nil {
		log.Printf("canned insight provenance error: %v", err)
		sendJSON("error", map[string]string{
			"message": "failed to build insight provenance",
		})
		return
	}
	provJSON, err := json.Marshal(prov)
	if err != nil {
		log.Printf("canned insight provenance JSON error: %v", err)
		sendJSON("error", map[string]string{
			"message": "failed to encode insight provenance",
		})
		return
	}
	structuredJSON, err := json.Marshal(envelope)
	if err != nil {
		log.Printf("canned insight structured JSON error: %v", err)
		sendJSON("error", map[string]string{
			"message": "failed to encode generated insight",
		})
		return
	}

	var project *string
	if req.Project != "" {
		project = &req.Project
	}
	var modelPtr *string
	if model != "" {
		modelPtr = &model
	}
	var promptPtr *string
	if req.Prompt != "" {
		promptPtr = &req.Prompt
	}

	id, err := s.db.InsertInsight(db.Insight{
		Type:            insight.CannedType,
		DateFrom:        req.DateFrom,
		DateTo:          req.DateTo,
		Project:         project,
		Agent:           result.Agent,
		Model:           modelPtr,
		Prompt:          promptPtr,
		Content:         insight.RenderCannedMarkdown(envelope, prov),
		Kind:            string(kind),
		SchemaVersion:   insight.CannedSchemaVersion,
		TemplateID:      prov.TemplateID,
		TemplateVersion: prov.TemplateVersion,
		AggregateHash:   aggregateHash,
		CacheKey:        cacheKey,
		CacheStatus:     "fresh",
		ProvenanceJSON:  string(provJSON),
		StructuredJSON:  string(structuredJSON),
	})
	if err != nil {
		log.Printf("canned insight insert error: %v", err)
		sendJSON("error", map[string]string{
			"message": "failed to save insight",
		})
		return
	}

	saved, err := s.db.GetInsight(r.Context(), id)
	if err != nil || saved == nil {
		log.Printf("canned insight get error: id=%d err=%v",
			id, err)
		sendJSON("error", map[string]string{
			"message": "failed to retrieve saved insight",
		})
		return
	}
	sendJSON("done", saved)
}

func markInsightCacheHit(s *db.Insight) {
	if s == nil {
		return
	}
	s.CacheStatus = "hit"
	if strings.TrimSpace(s.ProvenanceJSON) == "" {
		return
	}
	var prov map[string]any
	if err := json.Unmarshal([]byte(s.ProvenanceJSON), &prov); err != nil {
		return
	}
	if prov == nil {
		return
	}
	prov["cache_status"] = "hit"
	data, err := json.Marshal(prov)
	if err != nil {
		return
	}
	s.ProvenanceJSON = string(data)
}

func (s *Server) buildCannedPayload(
	ctx context.Context,
	kind insight.CannedKind,
	req generateInsightRequest,
) (insight.CannedAggregatePayload, string, string, error) {
	analyticsFilter := db.AnalyticsFilter{
		From:           req.DateFrom,
		To:             req.DateTo,
		Project:        req.Project,
		Timezone:       "UTC",
		ExcludeOneShot: true,
		AutomatedScope: req.AutomatedScope,
	}
	signals, err := s.db.GetAnalyticsSignals(ctx, analyticsFilter)
	if err != nil {
		return insight.CannedAggregatePayload{}, "", "", err
	}

	usageFilter := db.UsageFilter{
		From:           req.DateFrom,
		To:             req.DateTo,
		Project:        req.Project,
		Timezone:       "UTC",
		ExcludeOneShot: true,
		AutomatedScope: req.AutomatedScope,
		Breakdowns:     false,
	}
	usageResult, err := s.db.GetDailyUsage(ctx, usageFilter)
	if err != nil {
		return insight.CannedAggregatePayload{}, "", "", err
	}
	topSessions, err := s.db.GetTopSessionsByCost(ctx, usageFilter, 5)
	if err != nil {
		return insight.CannedAggregatePayload{}, "", "", err
	}
	usageSummary := &insight.CannedUsageSummary{
		InputTokens:         usageResult.Totals.InputTokens,
		OutputTokens:        usageResult.Totals.OutputTokens,
		CacheCreationTokens: usageResult.Totals.CacheCreationTokens,
		CacheReadTokens:     usageResult.Totals.CacheReadTokens,
		TotalCost:           usageResult.Totals.TotalCost,
		CacheSavings:        usageResult.Totals.CacheSavings,
		ModelBreakdowns:     foldCannedModelBreakdowns(usageResult.Daily),
		TopSessionsByCost:   topSessions,
	}
	coachSessions, err := s.listCannedCoachSessions(ctx, req)
	if err != nil {
		return insight.CannedAggregatePayload{}, "", "", err
	}
	coachSummary := insight.BuildCannedCoachSummary(coachSessions)

	payload := insight.CannedAggregatePayload{
		Kind:           kind,
		DateFrom:       req.DateFrom,
		DateTo:         req.DateTo,
		Project:        req.Project,
		AutomatedScope: req.AutomatedScope,
		Focus:          req.Prompt,
		Signals:        signals,
		Usage:          usageSummary,
		Coach:          coachSummary,
	}
	payload.EvidenceRefs = insight.CannedEvidenceRefs(
		signals, usageSummary, coachSummary,
	)

	aggregateHash, err := insight.CannedAggregateHash(payload)
	if err != nil {
		return insight.CannedAggregatePayload{}, "", "", err
	}
	cacheKey, err := insight.CannedCacheKey(
		kind, req.DateFrom, req.DateTo, req.Project,
		req.Agent, req.Prompt, aggregateHash,
		req.AutomatedScope,
	)
	if err != nil {
		return insight.CannedAggregatePayload{}, "", "", err
	}
	return payload, aggregateHash, cacheKey, nil
}

func foldCannedModelBreakdowns(
	daily []db.DailyUsageEntry,
) []insight.CannedModelBreakdown {
	type modelAccum struct {
		inputTok  int
		outputTok int
		cacheCr   int
		cacheRd   int
		cost      float64
	}
	byModel := make(map[string]*modelAccum)
	for _, day := range daily {
		for _, model := range day.ModelBreakdowns {
			acc, ok := byModel[model.ModelName]
			if !ok {
				acc = &modelAccum{}
				byModel[model.ModelName] = acc
			}
			acc.inputTok += model.InputTokens
			acc.outputTok += model.OutputTokens
			acc.cacheCr += model.CacheCreationTokens
			acc.cacheRd += model.CacheReadTokens
			acc.cost += model.Cost
		}
	}
	out := make([]insight.CannedModelBreakdown, 0, len(byModel))
	for model, acc := range byModel {
		out = append(out, insight.CannedModelBreakdown{
			ModelName:           model,
			InputTokens:         acc.inputTok,
			OutputTokens:        acc.outputTok,
			CacheCreationTokens: acc.cacheCr,
			CacheReadTokens:     acc.cacheRd,
			Cost:                acc.cost,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Cost != out[j].Cost {
			return out[i].Cost > out[j].Cost
		}
		return out[i].ModelName < out[j].ModelName
	})
	return out
}

func (s *Server) listCannedCoachSessions(
	ctx context.Context,
	req generateInsightRequest,
) ([]db.Session, error) {
	filter := db.SessionFilter{
		DateFrom:       req.DateFrom,
		DateTo:         req.DateTo,
		Project:        req.Project,
		ExcludeOneShot: true,
		AutomatedScope: req.AutomatedScope,
		Limit:          db.MaxSessionLimit,
	}
	var out []db.Session
	for {
		page, err := s.db.ListSessions(ctx, filter)
		if err != nil {
			return nil, err
		}
		out = append(out, page.Sessions...)
		if page.NextCursor == "" {
			return out, nil
		}
		filter.Cursor = page.NextCursor
	}
}
