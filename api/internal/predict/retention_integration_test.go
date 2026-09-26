//go:build integration

package predict

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// The archive must carry every gold.predictions column: the move names them
// explicitly, so a column added to the hot table must be added there too.
func TestArchiveColumnsMatchThePredictionsTable(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	cols := func(table string) []string {
		rows, err := pool.Query(ctx, `SELECT column_name FROM information_schema.columns
WHERE table_schema='gold' AND table_name=$1 AND column_name <> 'archived_at' ORDER BY ordinal_position`, table)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var c string
			_ = rows.Scan(&c)
			out = append(out, c)
		}
		return out
	}
	// Order is irrelevant (the move names every column on both sides; columns added
	// later by ALTER sit at the end physically), so compare as sets.
	hot, archive := cols("predictions"), cols("predictions_archive")
	listed := strings.Fields(strings.ReplaceAll(archiveColumns, ",", " "))
	sort.Strings(hot)
	sort.Strings(archive)
	sort.Strings(listed)
	if strings.Join(hot, ",") != strings.Join(archive, ",") || strings.Join(hot, ",") != strings.Join(listed, ",") {
		t.Fatalf("columns differ:\n hot      %v\n archive  %v\n moved    %v", hot, archive, listed)
	}
}

type retentionFixture struct {
	pool *pgxpool.Pool
	tag  string
	ids  map[string]int64
}

func (f *retentionFixture) insert(t *testing.T, name, source string, age time.Duration) {
	t.Helper()
	var id int64
	md := `{"batch":"test_split"}`
	if source != "" {
		md = fmt.Sprintf(`{"source":%q}`, source)
	}
	if err := f.pool.QueryRow(context.Background(), `
INSERT INTO gold.predictions (model_name, model_version, grain, entity_id, prediction, metadata, created_at)
VALUES ('fraud_risk', 'it-retention', 'order', $1, 0.5, $2::jsonb, now() - make_interval(secs => $3))
RETURNING id`, f.tag+"-"+name, md, age.Seconds()).Scan(&id); err != nil {
		t.Fatalf("insert %s: %v", name, err)
	}
	f.ids[name] = id
}

func TestRetentionArchivesOnlyWhatThePolicyAllows(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	if err := EnsureSchema(ctx, pool); err != nil {
		t.Fatal(err)
	}
	f := &retentionFixture{pool: pool, tag: fmt.Sprintf("it-ret-%d", time.Now().UnixNano()), ids: map[string]int64{}}
	day := 24 * time.Hour

	// Save + restore the shared reconcile cursor and clean every row we create.
	var savedCursor []byte
	hadCursor := pool.QueryRow(ctx, `SELECT value::text FROM gold.governance_state WHERE key='reconcile'`).Scan(&savedCursor) == nil
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM gold.predictions WHERE entity_id LIKE $1`, f.tag+"%")
		_, _ = pool.Exec(ctx, `DELETE FROM gold.predictions_archive WHERE entity_id LIKE $1`, f.tag+"%")
		_, _ = pool.Exec(ctx, `DELETE FROM gold.action_queue WHERE dedup_key LIKE $1`, f.tag+"%")
		if hadCursor {
			_, _ = pool.Exec(ctx, `UPDATE gold.governance_state SET value=$1::jsonb WHERE key='reconcile'`, string(savedCursor))
		} else {
			_, _ = pool.Exec(ctx, `DELETE FROM gold.governance_state WHERE key='reconcile'`)
		}
	})

	f.insert(t, "stream-old", "stream_score", 40*day)     // archived
	f.insert(t, "stream-new", "stream_score", 10*day)     // too young
	f.insert(t, "live-old", "live_score", 100*day)        // archived
	f.insert(t, "live-mid", "live_score", 40*day)         // live window is 90 days
	f.insert(t, "heldout-old", "", 400*day)               // trainer evidence: never
	f.insert(t, "churn-old", "churn_score", 400*day)      // batch source: never
	f.insert(t, "cited-pending", "stream_score", 40*day)  // trigger of a pending action
	f.insert(t, "cited-failed", "stream_score", 40*day)   // trigger of a failed action
	f.insert(t, "cited-executed", "stream_score", 40*day) // decided action: may go
	cursor := f.ids["cited-executed"]
	f.insert(t, "above-cursor", "stream_score", 40*day) // backstop hasn't seen it

	if _, err := pool.Exec(ctx, `
INSERT INTO gold.governance_state (key, value) VALUES ('reconcile', jsonb_build_object('predictions', $1::bigint, 'drift', 0))
ON CONFLICT (key) DO UPDATE SET value = jsonb_set(gold.governance_state.value, '{predictions}', to_jsonb($1::bigint))`, cursor); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name, status string
	}{{"cited-pending", "pending"}, {"cited-failed", "failed"}, {"cited-executed", "executed"}} {
		if _, err := pool.Exec(ctx, `
INSERT INTO gold.action_queue (action, entity, risk_tier, status, rule, dedup_key, trigger)
VALUES ('hold_order_for_review', $1, 'approval_required', $2, 'test', $3,
        jsonb_build_object('payload', jsonb_build_object('prediction', jsonb_build_object('id', $4::bigint))))`,
			c.name, c.status, f.tag+"|"+c.name, f.ids[c.name]); err != nil {
			t.Fatal(err)
		}
	}

	old := retentionChunk
	retentionChunk = 1 // force one statement per row: chunking must not change the outcome
	t.Cleanup(func() { retentionChunk = old })

	res, err := NewRetention(pool, slog.New(slog.DiscardHandler)).Once(ctx)
	if err != nil {
		t.Fatal(err)
	}
	wantMoved := map[string]bool{"stream-old": true, "live-old": true, "cited-executed": true}
	for name, id := range f.ids {
		var hot, archived int
		_ = pool.QueryRow(ctx, `SELECT count(*) FROM gold.predictions WHERE id=$1`, id).Scan(&hot)
		_ = pool.QueryRow(ctx, `SELECT count(*) FROM gold.predictions_archive WHERE id=$1`, id).Scan(&archived)
		if wantMoved[name] && (hot != 0 || archived != 1) {
			t.Errorf("%s: hot=%d archived=%d, want moved to the archive", name, hot, archived)
		}
		if !wantMoved[name] && (hot != 1 || archived != 0) {
			t.Errorf("%s: hot=%d archived=%d, want kept hot", name, hot, archived)
		}
	}
	if res.Moved < 3 || res.Chunks < 3 {
		t.Errorf("result %+v; want >= 3 rows moved in >= 3 chunks", res)
	}
	var logged int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM gold.event_log WHERE kind='retention' AND created_at > now() - interval '1 minute'`).Scan(&logged)
	if logged == 0 {
		t.Error("the run must write a gold.event_log accounting row")
	}
	// The archived row is intact (same payload, not a stub).
	var md string
	if err := pool.QueryRow(ctx, `SELECT metadata->>'source' FROM gold.predictions_archive WHERE id=$1`, f.ids["live-old"]).Scan(&md); err != nil || md != "live_score" {
		t.Errorf("archived row lost its metadata: %q %v", md, err)
	}
	// Idempotent: nothing left to move among our rows.
	res2, err := NewRetention(pool, slog.New(slog.DiscardHandler)).Once(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var stillEligible int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM gold.predictions WHERE id = ANY($1)`,
		[]int64{f.ids["stream-old"], f.ids["live-old"], f.ids["cited-executed"]}).Scan(&stillEligible)
	if stillEligible != 0 {
		t.Errorf("second run left %d eligible rows (moved %d)", stillEligible, res2.Moved)
	}
}

func TestRetentionMovesNothingWithoutAReconcileCursor(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	if err := EnsureSchema(ctx, pool); err != nil {
		t.Fatal(err)
	}
	var saved []byte
	had := pool.QueryRow(ctx, `SELECT value::text FROM gold.governance_state WHERE key='reconcile'`).Scan(&saved) == nil
	if had {
		_, _ = pool.Exec(ctx, `DELETE FROM gold.governance_state WHERE key='reconcile'`)
		t.Cleanup(func() {
			_, _ = pool.Exec(ctx, `INSERT INTO gold.governance_state (key, value) VALUES ('reconcile', $1::jsonb)
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`, string(saved))
		})
	}
	tag := fmt.Sprintf("it-nocur-%d", time.Now().UnixNano())
	var id int64
	_ = pool.QueryRow(ctx, `INSERT INTO gold.predictions (model_name, model_version, grain, entity_id, prediction, metadata, created_at)
VALUES ('fraud_risk','it','order',$1,0.5,'{"source":"stream_score"}', now() - interval '400 days') RETURNING id`, tag).Scan(&id)
	t.Cleanup(func() { _, _ = pool.Exec(ctx, `DELETE FROM gold.predictions WHERE entity_id=$1`, tag) })
	if _, err := NewRetention(pool, slog.New(slog.DiscardHandler)).Once(ctx); err != nil {
		t.Fatal(err)
	}
	var hot int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM gold.predictions WHERE id=$1`, id).Scan(&hot)
	if hot != 1 {
		t.Fatal("with no reconcile cursor nothing is eligible: the backstop has not considered any row yet")
	}
}
