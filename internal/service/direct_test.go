package service_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wesm/agentsview/internal/db"
	"github.com/wesm/agentsview/internal/dbtest"
	"github.com/wesm/agentsview/internal/service"
	"github.com/wesm/agentsview/internal/sync"
)

// directTestEnv is a lightweight environment helper for testing
// the directBackend. It holds the underlying *db.DB so test
// cases can seed fixture rows directly.
type directTestEnv struct {
	db *db.DB
}

// InsertSession upserts a minimal session row and returns its ID.
// Callers can use the returned ID to exercise the Get/List APIs
// without having to parse a real session fixture.
func (e *directTestEnv) InsertSession(t *testing.T) string {
	t.Helper()
	const sid = "test-session-1"
	dbtest.SeedSession(t, e.db, sid, "p1")
	return sid
}

// newDirectTestSvc builds a SessionService backed by an in-memory
// SQLite database with a nil sync engine (so Sync returns
// db.ErrReadOnly, matching the PG-serve read path).
func newDirectTestSvc(t *testing.T) (service.SessionService, *directTestEnv) {
	t.Helper()
	d := dbtest.OpenTestDB(t)
	return service.NewDirectBackend(d, nil), &directTestEnv{db: d}
}

func TestDirectBackend_Get_Roundtrip(t *testing.T) {
	t.Parallel()
	svc, env := newDirectTestSvc(t)
	sessionID := env.InsertSession(t)

	detail, err := svc.Get(context.Background(), sessionID)
	require.NoError(t, err)
	require.NotNil(t, detail)
	assert.Equal(t, sessionID, detail.ID)
}

func TestDirectBackend_Get_HealthBreakdownIncludesHeuristics(
	t *testing.T,
) {
	t.Parallel()
	svc, env := newDirectTestSvc(t)
	sessionID := env.InsertSession(t)
	dbtest.SeedMessages(t, env.db,
		dbtest.UserMsg(sessionID, 0,
			"Fix the backend test failure in the codebase."),
		dbtest.AsstMsg(sessionID, 1, "I'll inspect it."),
		dbtest.UserMsg(sessionID, 2,
			"Fix the backend test failure in the codebase."),
	)
	score := 90
	grade := "A"
	err := env.db.UpdateSessionSignals(
		sessionID,
		db.SessionSignalUpdate{
			Outcome:           "completed",
			OutcomeConfidence: "high",
			EndedWithRole:     "assistant",
			HealthScore:       &score,
			HealthGrade:       &grade,
			QualitySignals: db.QualitySignals{
				Version:              db.CurrentQualitySignalVersion,
				DuplicatePromptCount: 1,
				NoCodeContextCount:   1,
			},
		},
	)
	require.NoError(t, err)

	detail, err := svc.Get(context.Background(), sessionID)
	require.NoError(t, err)
	require.NotNil(t, detail)

	assert.Contains(t, detail.HealthScoreBasis, "prompt_quality")
	assert.Contains(t, detail.HealthScoreBasis, "context_quality")
	assert.NotContains(t, detail.HealthPenalties, "repeated_prompts")
	assert.NotContains(t, detail.HealthPenalties, "stuck_repeated_prompts")
	assert.Equal(t, 4,
		detail.HealthPenalties["code_task_without_context"])
}

func TestDirectBackend_List_Empty(t *testing.T) {
	t.Parallel()
	svc, _ := newDirectTestSvc(t)
	list, err := svc.List(context.Background(), service.ListFilter{Limit: 10})
	require.NoError(t, err)
	assert.Equal(t, 0, list.Total)
}

func TestDirectBackend_List_InvalidDate(t *testing.T) {
	t.Parallel()
	svc, _ := newDirectTestSvc(t)

	cases := []struct {
		name   string
		filter service.ListFilter
		want   string
	}{
		{
			name:   "Date bad format",
			filter: service.ListFilter{Date: "2024/01/15"},
			want:   `invalid date "2024/01/15"`,
		},
		{
			name:   "DateFrom bad format",
			filter: service.ListFilter{DateFrom: "not-a-date"},
			want:   `invalid date "not-a-date"`,
		},
		{
			name:   "DateTo bad format",
			filter: service.ListFilter{DateTo: "2024-13-40"},
			want:   `invalid date "2024-13-40"`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			list, err := svc.List(context.Background(), tc.filter)
			require.Error(t, err)
			assert.Nil(t, list)
			assert.Contains(t, err.Error(), tc.want)
			assert.Contains(t, err.Error(), "YYYY-MM-DD")
		})
	}
}

func TestDirectBackend_List_DateFromAfterDateTo(t *testing.T) {
	t.Parallel()
	svc, _ := newDirectTestSvc(t)

	list, err := svc.List(context.Background(), service.ListFilter{
		DateFrom: "2024-12-01",
		DateTo:   "2024-01-01",
	})
	require.Error(t, err)
	assert.Nil(t, list)
	assert.Contains(t, err.Error(), "date_from must not be after date_to")
}

func TestDirectBackend_List_InvalidActiveSince(t *testing.T) {
	t.Parallel()
	svc, _ := newDirectTestSvc(t)

	list, err := svc.List(context.Background(), service.ListFilter{
		ActiveSince: "yesterday",
	})
	require.Error(t, err)
	assert.Nil(t, list)
	assert.Contains(t, err.Error(), `invalid active_since "yesterday"`)
	assert.Contains(t, err.Error(), "RFC3339")
}

func TestDirectBackend_List_ValidDatesAccepted(t *testing.T) {
	t.Parallel()
	svc, _ := newDirectTestSvc(t)

	list, err := svc.List(context.Background(), service.ListFilter{
		Date:        "2024-06-15",
		DateFrom:    "2024-01-01",
		DateTo:      "2024-12-31",
		ActiveSince: "2024-06-15T12:30:45Z",
	})
	require.NoError(t, err)
	require.NotNil(t, list)
}

// TestDirectBackend_List_ClampsOverMaxLimit verifies that a caller
// passing a Limit larger than db.MaxSessionLimit is clamped to
// MaxSessionLimit rather than being reset to DefaultSessionLimit
// (which is the raw db.ListSessions guard's behavior). This matches
// the HTTP handler's clampLimit semantics.
func TestDirectBackend_List_ClampsOverMaxLimit(t *testing.T) {
	t.Parallel()
	svc, env := newDirectTestSvc(t)

	// Seed DefaultSessionLimit+1 sessions so we can distinguish
	// "clamped to MaxSessionLimit" (>DefaultSessionLimit returned)
	// from "reset to DefaultSessionLimit" (only DefaultSessionLimit
	// returned).
	nSessions := db.DefaultSessionLimit + 1
	for i := range nSessions {
		dbtest.SeedSession(
			t, env.db, fmt.Sprintf("s-%04d", i), "p1",
		)
	}

	list, err := svc.List(context.Background(), service.ListFilter{
		Limit:          db.MaxSessionLimit + 500,
		IncludeOneShot: true, // seeded sessions have 1 message each
	})
	require.NoError(t, err)
	require.NotNil(t, list)
	// If the clamp works, we get all nSessions back (since
	// nSessions < MaxSessionLimit). Without the clamp, we would
	// only get DefaultSessionLimit back.
	assert.Equal(t, nSessions, len(list.Sessions),
		"limit should clamp to MaxSessionLimit, not reset to default")
}

func TestDirectBackend_Sync_BothPathAndID(t *testing.T) {
	t.Parallel()
	d := dbtest.OpenTestDB(t)
	// Ephemeral sync engine: enough to pass the nil-engine guard
	// and reach the validation branch. We never call SyncPaths in
	// this test because validation fails first.
	engine := sync.NewEngine(d, sync.EngineConfig{Ephemeral: true})
	svc := service.NewDirectBackend(d, engine)

	_, err := svc.Sync(context.Background(), service.SyncInput{
		Path: "/tmp/session.jsonl",
		ID:   "abc123",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "only one of path or id allowed")
}

func TestDirectBackend_Sync_NilEngineIsReadOnly(t *testing.T) {
	t.Parallel()
	svc, _ := newDirectTestSvc(t)

	_, err := svc.Sync(context.Background(), service.SyncInput{
		Path: "/tmp/session.jsonl",
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, db.ErrReadOnly),
		"expected db.ErrReadOnly, got %v", err)
}

// TestDirectBackend_Sync_AmbiguousPath_ReturnsListedIDs verifies
// that when one JSONL file maps to multiple sessions in the DB
// (e.g. Claude forked transcripts), Sync refuses to pick one
// arbitrarily and instead returns an error naming every candidate
// id, telling the caller to disambiguate via `session sync <id>`.
func TestDirectBackend_Sync_AmbiguousPath_ReturnsListedIDs(t *testing.T) {
	t.Parallel()
	d := dbtest.OpenTestDB(t)
	// Ephemeral engine so SyncPaths is a no-op — the test only
	// exercises the post-sync resolver.
	engine := sync.NewEngine(d, sync.EngineConfig{Ephemeral: true})
	svc := service.NewDirectBackend(d, engine)

	path := "/tmp/forked-session.jsonl"
	for _, id := range []string{"fork-a", "fork-b"} {
		require.NoError(t, d.UpsertSession(db.Session{
			ID:       id,
			Project:  "proj",
			Machine:  "local",
			Agent:    "claude",
			FilePath: &path,
		}))
	}

	_, err := svc.Sync(context.Background(), service.SyncInput{
		Path: path,
	})
	require.Error(t, err)
	msg := err.Error()
	assert.Contains(t, msg, "2 sessions found",
		"error should state the ambiguity count")
	assert.Contains(t, msg, "fork-a")
	assert.Contains(t, msg, "fork-b")
	assert.Contains(t, msg, "session sync <id>",
		"error should tell the caller how to disambiguate")
}

// TestDirectBackend_Watch_UnknownID_Errors verifies that Watch
// on a missing session returns a clear "session not found" error
// instead of producing an indefinite heartbeat channel.
func TestDirectBackend_Watch_UnknownID_Errors(t *testing.T) {
	t.Parallel()
	svc, _ := newDirectTestSvc(t)

	_, err := svc.Watch(context.Background(), "does-not-exist")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "session not found")
	assert.Contains(t, err.Error(), "does-not-exist")
}

// TestDirectBackend_Messages_InvalidDirection verifies that the
// service layer rejects direction values outside {asc, desc}. HTTP
// and CLI both route through this, so the contract is enforced
// uniformly.
func TestDirectBackend_Messages_InvalidDirection(t *testing.T) {
	t.Parallel()
	svc, _ := newDirectTestSvc(t)

	_, err := svc.Messages(context.Background(), "sid",
		service.MessageFilter{Direction: "backwards"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid direction")
	assert.Contains(t, err.Error(), "backwards")
}

// TestReadOnlyBackend_Sync_IsReadOnly verifies that a backend
// constructed via NewReadOnlyBackend rejects Sync with
// db.ErrReadOnly regardless of the input.
func TestReadOnlyBackend_Sync_IsReadOnly(t *testing.T) {
	t.Parallel()
	d := dbtest.OpenTestDB(t)
	// Pass the *db.DB as a db.Store; the constructor's type
	// parameter restricts Sync capability, not read access.
	var store db.Store = d
	svc := service.NewReadOnlyBackend(store)

	_, err := svc.Sync(context.Background(), service.SyncInput{
		Path: "/tmp/session.jsonl",
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, db.ErrReadOnly),
		"expected db.ErrReadOnly, got %v", err)
}

// TestDirectBackend_Messages_DescOmittedFrom exercises the
// "omitted From in desc mode == newest page" branch: when the
// filter's From pointer is nil, the backend promotes it to
// MaxInt32 so a descending query returns the newest messages.
func TestDirectBackend_Messages_DescOmittedFrom(t *testing.T) {
	t.Parallel()
	svc, env := newDirectTestSvc(t)
	sid := env.InsertSession(t)

	// Seed 5 user messages, ordinals 0..4.
	msgs := make([]db.Message, 0, 5)
	for i := range 5 {
		msgs = append(msgs, dbtest.UserMsg(sid, i, fmt.Sprintf("m%d", i)))
	}
	dbtest.SeedMessages(t, env.db, msgs...)

	list, err := svc.Messages(context.Background(), sid, service.MessageFilter{
		Direction: "desc",
		Limit:     10,
	})
	require.NoError(t, err)
	require.NotNil(t, list)
	require.Equal(t, 5, list.Count)
	for i, m := range list.Messages {
		wantOrd := 4 - i
		assert.Equal(t, wantOrd, m.Ordinal,
			"desc iteration should return highest ordinal first")
	}
	assert.True(t, strings.HasPrefix(list.Messages[0].Content, "m4"))
}

// TestDirectBackend_Messages_DescExplicitZeroFrom verifies that an
// explicit From=0 in descending mode starts at ordinal 0 (returning
// only the ordinal-0 message) rather than being treated as "omitted"
// and promoted to MaxInt32.
func TestDirectBackend_Messages_DescExplicitZeroFrom(t *testing.T) {
	t.Parallel()
	svc, env := newDirectTestSvc(t)
	sid := env.InsertSession(t)

	msgs := make([]db.Message, 0, 5)
	for i := range 5 {
		msgs = append(msgs, dbtest.UserMsg(sid, i, fmt.Sprintf("m%d", i)))
	}
	dbtest.SeedMessages(t, env.db, msgs...)

	zero := 0
	list, err := svc.Messages(context.Background(), sid, service.MessageFilter{
		Direction: "desc",
		From:      &zero,
		Limit:     10,
	})
	require.NoError(t, err)
	require.NotNil(t, list)
	require.Equal(t, 1, list.Count,
		"explicit From=0 in desc should start at ordinal 0 and "+
			"return only that message")
	assert.Equal(t, 0, list.Messages[0].Ordinal)
}
