package main

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

type result struct {
	PG             string  `json:"pg"`
	Section        string  `json:"section"`
	Value          int     `json:"value"`
	Variant        string  `json:"variant"`
	Xip            int     `json:"xip"`
	Idle           int     `json:"idle"`
	Callers        int     `json:"callers"`
	LongTxRows     int     `json:"longtx_rows"`
	JournalRows    int64   `json:"journal_rows"`
	XipObserved    int     `json:"xip_observed"`
	Calls          int64   `json:"calls"`
	Throughput     float64 `json:"throughput"`
	P50Ms          float64 `json:"p50_ms"`
	P99Ms          float64 `json:"p99_ms"`
	P999Ms         float64 `json:"p999_ms"`
	ServerMeanMs   float64 `json:"server_mean_ms"`
	ServerStddevMs float64 `json:"server_stddev_ms"`
	Buffers        int64   `json:"buffers"`
	HeapFetches    int64   `json:"heap_fetches"`
	ExplainExecMs  float64 `json:"explain_exec_ms"`
	SdbRate        float64 `json:"sdb_rate"`
	ForeignRate    float64 `json:"foreign_rate"`
}

func measure(ctx context.Context, cfg config, admin *pgx.Conn, bg *background, v variant, p point) (result, error) {
	r := result{PG: cfg.pg, Section: p.Section, Value: p.Value, Variant: v.name, Xip: p.Xip, Idle: p.Idle, Callers: p.Callers, LongTxRows: p.LongTxRows}

	conns := make([]*pgx.Conn, 0, p.Callers)
	defer func() {
		for _, c := range conns {
			_ = c.Close(context.Background())
		}
	}()
	for range p.Callers {
		c, err := pgx.Connect(ctx, cfg.sdbDSN)
		if err != nil {
			return r, fmt.Errorf("opening caller connection: %w", err)
		}
		conns = append(conns, c)
	}
	call := func(c *pgx.Conn) error {
		rows, err := c.Query(ctx, v.sql)
		if err != nil {
			return err
		}
		rows.Close()
		return rows.Err()
	}

	if _, err := hammer(ctx, conns, call, cfg.warm, nil); err != nil {
		return r, fmt.Errorf("warmup: %w", err)
	}
	if _, err := admin.Exec(ctx, `SELECT pg_stat_statements_reset()`); err != nil {
		return r, fmt.Errorf("resetting pg_stat_statements: %w", err)
	}
	if err := admin.QueryRow(ctx, `SELECT cardinality(ARRAY(SELECT pg_snapshot_xip(pg_current_snapshot())))`).Scan(&r.XipObserved); err != nil {
		return r, fmt.Errorf("sampling xip: %w", err)
	}

	sdb0, foreign0 := bg.sdbWrites.Load(), bg.foreignCommits.Load()
	t0 := time.Now()
	progress := func(calls int64, elapsed time.Duration) {
		fmt.Printf("   %-8s %3.0fs/%.0fs  calls=%d  %.0f/s\n", v.name, elapsed.Seconds(), cfg.dur.Seconds(), calls, float64(calls)/elapsed.Seconds())
	}
	lat, err := hammer(ctx, conns, call, cfg.dur, progress)
	if err != nil {
		return r, err
	}
	elapsed := time.Since(t0).Seconds()
	r.SdbRate = float64(bg.sdbWrites.Load()-sdb0) / elapsed
	r.ForeignRate = float64(bg.foreignCommits.Load()-foreign0) / elapsed

	slices.Sort(lat)
	r.Calls = int64(len(lat))
	r.Throughput = float64(len(lat)) / elapsed
	r.P50Ms, r.P99Ms, r.P999Ms = percentile(lat, 0.5), percentile(lat, 0.99), percentile(lat, 0.999)

	err = admin.QueryRow(ctx, `
		SELECT mean_exec_time, stddev_exec_time FROM pg_stat_statements
		WHERE query LIKE $1 ORDER BY calls DESC LIMIT 1`, "%"+v.match+"%").
		Scan(&r.ServerMeanMs, &r.ServerStddevMs)
	if err != nil {
		return r, fmt.Errorf("reading pg_stat_statements: %w", err)
	}
	if r.Buffers, r.HeapFetches, r.ExplainExecMs, err = explain(ctx, admin, v.sql, cfg.explainRuns); err != nil {
		return r, err
	}
	if r.JournalRows, err = journalRows(ctx, admin); err != nil {
		return r, err
	}
	return r, nil
}

func hammer(ctx context.Context, conns []*pgx.Conn, call func(*pgx.Conn) error, d time.Duration, progress func(int64, time.Duration)) ([]float64, error) {
	deadline := time.Now().Add(d)
	var total atomic.Int64
	per := make([][]float64, len(conns))
	errs := make([]error, len(conns))
	var wg sync.WaitGroup
	for i, c := range conns {
		wg.Go(func() {
			for time.Now().Before(deadline) && ctx.Err() == nil {
				t := time.Now()
				if err := call(c); err != nil {
					errs[i] = err
					return
				}
				per[i] = append(per[i], float64(time.Since(t).Nanoseconds())/1e6)
				total.Add(1)
			}
		})
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	start := time.Now()
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
wait:
	for {
		select {
		case <-done:
			break wait
		case <-tick.C:
			if progress != nil {
				progress(total.Load(), time.Since(start))
			}
		}
	}

	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return slices.Concat(per...), nil
}

func percentile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[int(q*float64(len(sorted)-1))]
}

func explain(ctx context.Context, admin *pgx.Conn, sql string, runs int) (int64, int64, float64, error) {
	bufs := make([]int64, 0, runs)
	fetches := make([]int64, 0, runs)
	execs := make([]float64, 0, runs)
	for range runs {
		var raw string
		if err := admin.QueryRow(ctx, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+sql).Scan(&raw); err != nil {
			return 0, 0, 0, fmt.Errorf("explain: %w", err)
		}
		var out []struct {
			Plan          map[string]any `json:"Plan"`
			ExecutionTime float64        `json:"Execution Time"`
		}
		if err := json.Unmarshal([]byte(raw), &out); err != nil || len(out) != 1 {
			return 0, 0, 0, fmt.Errorf("parsing explain output: %w", err)
		}
		bufs = append(bufs, int64(num(out[0].Plan, "Shared Hit Blocks")+num(out[0].Plan, "Shared Read Blocks")))
		fetches = append(fetches, int64(sumHeapFetches(out[0].Plan)))
		execs = append(execs, out[0].ExecutionTime)
	}
	slices.Sort(bufs)
	slices.Sort(fetches)
	slices.Sort(execs)
	return bufs[len(bufs)/2], fetches[len(fetches)/2], execs[len(execs)/2], nil
}

func num(node map[string]any, key string) float64 {
	v, _ := node[key].(float64)
	return v
}

func sumHeapFetches(node map[string]any) float64 {
	total := num(node, "Heap Fetches")
	children, _ := node["Plans"].([]any)
	for _, child := range children {
		if m, ok := child.(map[string]any); ok {
			total += sumHeapFetches(m)
		}
	}
	return total
}
