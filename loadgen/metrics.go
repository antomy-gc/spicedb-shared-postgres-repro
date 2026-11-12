package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type sample struct {
	counters map[string]float64
	xmax     int64
	txns     int64
}

func snapshot(ctx context.Context, cfg config) (sample, error) {
	s := sample{counters: map[string]float64{}}
	for _, u := range cfg.metrics {
		if err := scrape(ctx, u, s.counters); err != nil {
			return s, err
		}
	}
	conn, err := pgx.Connect(ctx, cfg.pgURI)
	if err != nil {
		return s, err
	}
	defer conn.Close(ctx)
	err = conn.QueryRow(ctx, `SELECT pg_snapshot_xmax(pg_current_snapshot())::text::bigint, (SELECT count(*) FROM relation_tuple_transaction)`).Scan(&s.xmax, &s.txns)
	return s, err
}

var wanted = []string{
	"spicedb_cache_hits_total",
	"spicedb_cache_misses_total",
	"spicedb_datastore_query_latency_count",
	"spicedb_datastore_query_latency_sum",
	"spicedb_dispatch_client_check_total",
	"spicedb_dispatch_client_check_from_cache_total",
	"spicedb_dispatch_check_total",
	"spicedb_dispatch_check_from_cache_total",
	"spicedb_dispatch_remote_dispatch_handler_total",
}

func scrape(ctx context.Context, url string, into map[string]float64) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || line[0] == '#' {
			continue
		}
		name, rest, _ := strings.Cut(line, "{")
		if rest == "" {
			name, rest, _ = strings.Cut(line, " ")
			rest = " " + rest
		}
		keep := false
		for _, w := range wanted {
			if name == w {
				keep = true
				break
			}
		}
		if !keep {
			continue
		}
		labels, valueStr, _ := strings.Cut(rest, " ")
		key := name
		if cache := labelValue(labels, "cache"); cache != "" {
			key += "{cache=" + cache + "}"
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(valueStr), 64)
		if err != nil {
			continue
		}
		into[key] += v
	}
	return sc.Err()
}

func labelValue(labels, name string) string {
	for _, kv := range strings.Split(strings.TrimSuffix(labels, "}"), ",") {
		k, v, ok := strings.Cut(kv, "=")
		if ok && k == name {
			return strings.Trim(v, `"`)
		}
	}
	return ""
}

func report(w io.Writer, before, after sample, d time.Duration) {
	secs := d.Seconds()
	delta := func(k string) float64 { return after.counters[k] - before.counters[k] }
	ratio := func(hit, miss string) string {
		h, m := delta(hit), delta(miss)
		if h+m == 0 {
			return "n/a"
		}
		return fmt.Sprintf("%.1f%%", 100*h/(h+m))
	}
	fmt.Fprintf(w, "  postgres xids advanced: %d (%.0f/s); spicedb transactions: %d\n",
		after.xmax-before.xmax, float64(after.xmax-before.xmax)/secs, after.txns-before.txns)
	var caches []string
	for k := range after.counters {
		if strings.HasPrefix(k, "spicedb_cache_hits_total{cache=") {
			caches = append(caches, strings.TrimSuffix(strings.TrimPrefix(k, "spicedb_cache_hits_total{cache="), "}"))
		}
	}
	sort.Strings(caches)
	for _, c := range caches {
		fmt.Fprintf(w, "  cache %-18s hit ratio %s\n", c, ratio("spicedb_cache_hits_total{cache="+c+"}", "spicedb_cache_misses_total{cache="+c+"}"))
	}
	share := func(part, total string) string {
		if delta(total) == 0 {
			return "n/a"
		}
		return fmt.Sprintf("%.1f%%", 100*delta(part)/delta(total))
	}
	fmt.Fprintf(w, "  dispatch client checks: %.0f/s, from cache %s\n",
		delta("spicedb_dispatch_client_check_total")/secs,
		share("spicedb_dispatch_client_check_from_cache_total", "spicedb_dispatch_client_check_total"))
	fmt.Fprintf(w, "  cluster dispatch checks: %.0f/s, from cache %s\n",
		delta("spicedb_dispatch_check_total")/secs,
		share("spicedb_dispatch_check_from_cache_total", "spicedb_dispatch_check_total"))
	q := delta("spicedb_datastore_query_latency_count")
	fmt.Fprintf(w, "  datastore queries: %.0f/s, %.2f s/s, avg %.2f ms\n",
		q/secs, delta("spicedb_datastore_query_latency_sum")/secs, 1000*delta("spicedb_datastore_query_latency_sum")/max(q, 1))
}
