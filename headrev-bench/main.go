package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"slices"
	"syscall"
	"time"
)

type config struct {
	sdbDSN      string
	noiseDSN    string
	pg          string
	warm        time.Duration
	dur         time.Duration
	explainRuns int
}

func main() {
	pg := flag.String("pg", "?", "postgres major version, recorded in results")
	mode := flag.String("mode", "full", "full or smoke")
	only := flag.String("only", "", "run a single point of the full plan, as section=value")
	flag.Parse()

	cfg := config{
		sdbDSN:      "host=/var/run/postgresql user=postgres dbname=spicedb",
		noiseDSN:    "host=/var/run/postgresql user=postgres dbname=noise",
		pg:          *pg,
		warm:        3 * time.Second,
		dur:         20 * time.Second,
		explainRuns: 15,
	}
	points := fullPlan()
	if *mode == "smoke" {
		points = smokePlan()
		cfg.warm, cfg.dur, cfg.explainRuns = time.Second, 5*time.Second, 3
	}
	if *only != "" {
		points = slices.DeleteFunc(points, func(p point) bool { return fmt.Sprintf("%s=%d", p.Section, p.Value) != *only })
		if len(points) == 0 {
			fmt.Fprintln(os.Stderr, "no point matches", *only)
			os.Exit(2)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg, points); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
