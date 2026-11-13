package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

type point struct {
	Section     string
	Value       int
	Journal     int
	Xip         int
	Idle        int
	Callers     int
	SdbRate     int
	ForeignRate int
	LongTxRows  int
}

func (p point) String() string {
	return fmt.Sprintf("%s=%d  (journal>=%d xip=%d idle=%d callers=%d spicedb-writes=%d/s foreign-commits=%d/s rows-under-open-txs=%d)",
		p.Section, p.Value, p.Journal, p.Xip, p.Idle, p.Callers, p.SdbRate, p.ForeignRate, p.LongTxRows)
}

var basePoint = point{Journal: 100_000, Xip: 10, Idle: 50, Callers: 8, SdbRate: 10, ForeignRate: 650}

func fullPlan() []point {
	var pts []point
	add := func(section string, values []int, set func(*point, int)) {
		for _, v := range values {
			p := basePoint
			p.Section, p.Value = section, v
			set(&p, v)
			pts = append(pts, p)
		}
	}
	add("journal", []int{1_000}, func(p *point, v int) { p.Journal = v })
	add("xip", []int{0, 10, 50, 100, 200, 400}, func(p *point, v int) { p.Xip = v })
	add("idle", []int{10, 200, 800}, func(p *point, v int) { p.Idle = v })
	add("callers", []int{1, 8, 32, 64}, func(p *point, v int) { p.Callers = v })
	add("spicedb-writes", []int{0, 10, 100, 1000}, func(p *point, v int) { p.SdbRate = v })
	add("foreign-commits", []int{0, 650, 5000}, func(p *point, v int) { p.ForeignRate = v })
	add("rows-under-open-txs", []int{1_000, 10_000, 100_000}, func(p *point, v int) { p.LongTxRows = v })
	add("journal", []int{1_000_000, 10_000_000}, func(p *point, v int) { p.Journal = v })
	return pts
}

func smokePlan() []point {
	base := basePoint
	base.Journal = 1_000
	pts := []point{base, base, base}
	pts[0].Section, pts[0].Value = "journal", 1_000
	pts[1].Section, pts[1].Value, pts[1].Xip = "xip", 50, 50
	pts[2].Section, pts[2].Value, pts[2].LongTxRows = "rows-under-open-txs", 2_000, 2_000
	return pts
}

func run(ctx context.Context, cfg config, points []point) error {
	admin, err := pgx.Connect(ctx, cfg.sdbDSN)
	if err != nil {
		return fmt.Errorf("connecting: %w", err)
	}
	defer func() { _ = admin.Close(context.Background()) }()

	perPoint := time.Duration(len(variants))*(cfg.warm+cfg.dur+time.Duration(cfg.explainRuns)*50*time.Millisecond) + 5*time.Second
	fmt.Printf("postgres %s, %d points x %d variants, %s warmup + %s measured per variant, ~%s without seeding\n",
		cfg.pg, len(points), len(variants), cfg.warm, cfg.dur, (time.Duration(len(points)) * perPoint).Round(time.Minute))
	for i, p := range points {
		fmt.Printf("  %2d. %s\n", i+1, p)
	}

	start := time.Now()
	var results []result
	for i, p := range points {
		fmt.Printf("\n== [%d/%d] %s\n   elapsed %s, remaining ~%s\n", i+1, len(points), p,
			time.Since(start).Round(time.Second), (time.Duration(len(points)-i) * perPoint).Round(time.Second))
		if err := ensureJournal(ctx, admin, p.Journal); err != nil {
			return err
		}
		bg, err := startBackground(ctx, cfg, p)
		if err != nil {
			return err
		}
		if p.LongTxRows > 0 {
			if err := appendJournal(ctx, admin, p.LongTxRows, "journal rows under open foreign txs"); err != nil {
				bg.stop()
				return err
			}
		}
		for _, v := range variants {
			r, err := measure(ctx, cfg, admin, bg, v, p)
			if err != nil {
				bg.stop()
				return fmt.Errorf("%s / %s: %w", p, v.name, err)
			}
			printResult(r)
			results = append(results, r)
		}
		bg.stop()
	}
	printSummary(results)
	fmt.Printf("\ndone in %s\n", time.Since(start).Round(time.Second))
	return nil
}

func printResult(r result) {
	fmt.Printf("   %-8s calls=%d (%.0f/s)  client p50=%.3f p99=%.3f p999=%.3f ms | server mean=%.3f sd=%.3f ms | buffers=%d heap-fetches=%d explain=%.3f ms | xip=%d journal=%d | bg spicedb=%.0f/s foreign=%.0f/s\n",
		r.Variant, r.Calls, r.Throughput, r.P50Ms, r.P99Ms, r.P999Ms, r.ServerMeanMs, r.ServerStddevMs,
		r.Buffers, r.HeapFetches, r.ExplainExecMs, r.XipObserved, r.JournalRows, r.SdbRate, r.ForeignRate)
	line, _ := json.Marshal(r)
	fmt.Printf("RESULT %s\n", line)
}

func printSummary(results []result) {
	fmt.Printf("\n== summary: server mean ms / buffers per call / client p99 ms\n")
	for i := 0; i < len(results); i += len(variants) {
		r := results[i]
		fmt.Printf("%-20s %8d |", r.Section, r.Value)
		for _, v := range results[i:min(i+len(variants), len(results))] {
			fmt.Printf(" %-8s %7.3f %6d %7.3f |", v.Variant, v.ServerMeanMs, v.Buffers, v.P99Ms)
		}
		fmt.Println()
	}
}
