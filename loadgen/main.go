package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/authzed-go/v1"
	"github.com/authzed/grpcutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	users      = 600
	groups     = 60
	resources = 2000
)

type config struct {
	endpoints     []string
	metrics       []string
	key           string
	pgURI         string
	mode          string
	rate          int
	bulk          int
	warmup        time.Duration
	duration      time.Duration
	writeInterval time.Duration
	seed          bool
}

func main() {
	var cfg config
	var endpoints, metrics string
	flag.StringVar(&endpoints, "endpoints", "localhost:50061,localhost:50062", "comma-separated SpiceDB gRPC endpoints")
	flag.StringVar(&metrics, "metrics", "http://localhost:9091/metrics,http://localhost:9092/metrics", "comma-separated metrics URLs")
	flag.StringVar(&cfg.key, "key", "repro", "preshared key")
	flag.StringVar(&cfg.pgURI, "pg", "postgres://spicedb:spicedb@localhost:5435/spicedb?sslmode=disable", "postgres URI for xid counters")
	flag.StringVar(&cfg.mode, "mode", "fully_consistent", "fully_consistent | minimize_latency | at_least_as_fresh")
	flag.IntVar(&cfg.rate, "rate", 20, "CheckBulkPermissions calls per second")
	flag.IntVar(&cfg.bulk, "bulk", 10, "items per CheckBulkPermissions call")
	flag.DurationVar(&cfg.warmup, "warmup", 15*time.Second, "warm-up before measuring")
	flag.DurationVar(&cfg.duration, "duration", 90*time.Second, "measured duration")
	flag.DurationVar(&cfg.writeInterval, "write-interval", 3*time.Second, "interval between relationship writes (0 = no writes)")
	flag.BoolVar(&cfg.seed, "seed", false, "write schema and relationships, then exit")
	zipfS := flag.Float64("zipf", 1.1, "Zipf exponent for resource and user selection (higher = hotter keys)")
	flag.Parse()
	zipfNS = rand.NewZipf(rand.New(rand.NewSource(1)), *zipfS, 1, resources-1)
	zipfUsr = rand.NewZipf(rand.New(rand.NewSource(2)), *zipfS, 1, users-1)
	cfg.endpoints = strings.Split(endpoints, ",")
	cfg.metrics = strings.Split(metrics, ",")

	ctx := context.Background()
	clients := make([]*authzed.Client, 0, len(cfg.endpoints))
	for _, ep := range cfg.endpoints {
		c, err := authzed.NewClient(ep,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpcutil.WithInsecureBearerToken(cfg.key))
		if err != nil {
			log.Fatalf("connect %s: %v", ep, err)
		}
		clients = append(clients, c)
	}

	if cfg.seed {
		if err := seed(ctx, clients[0]); err != nil {
			log.Fatal(err)
		}
		return
	}
	if err := run(ctx, cfg, clients); err != nil {
		log.Fatal(err)
	}
}

type lastWrite struct{ token atomic.Pointer[v1.ZedToken] }

func (l *lastWrite) consistency(mode string) *v1.Consistency {
	switch mode {
	case "minimize_latency":
		return &v1.Consistency{Requirement: &v1.Consistency_MinimizeLatency{MinimizeLatency: true}}
	case "at_least_as_fresh":
		if t := l.token.Load(); t != nil {
			return &v1.Consistency{Requirement: &v1.Consistency_AtLeastAsFresh{AtLeastAsFresh: t}}
		}
		return &v1.Consistency{Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true}}
	default:
		return &v1.Consistency{Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true}}
	}
}

func run(ctx context.Context, cfg config, clients []*authzed.Client) error {
	lw := &lastWrite{}
	if err := writeOne(ctx, clients[0], lw); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if cfg.writeInterval > 0 {
		go func() {
			t := time.NewTicker(cfg.writeInterval)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					if err := writeOne(ctx, clients[rand.Intn(len(clients))], lw); err != nil && ctx.Err() == nil {
						log.Printf("write: %v", err)
					}
				}
			}
		}()
	}

	var (
		mu        sync.Mutex
		latencies []time.Duration
		errs      atomic.Int64
		measuring atomic.Bool
	)
	var wg sync.WaitGroup
	tick := time.NewTicker(time.Second / time.Duration(cfg.rate))
	defer tick.Stop()
	loadCtx, stopLoad := context.WithCancel(ctx)
	defer stopLoad()
	go func() {
		i := 0
		for {
			select {
			case <-loadCtx.Done():
				return
			case <-tick.C:
				c := clients[i%len(clients)]
				i++
				wg.Add(1)
				go func() {
					defer wg.Done()
					start := time.Now()
					if err := checkBulk(loadCtx, c, cfg, lw); err != nil {
						if loadCtx.Err() == nil {
							errs.Add(1)
						}
						return
					}
					if measuring.Load() {
						mu.Lock()
						latencies = append(latencies, time.Since(start))
						mu.Unlock()
					}
				}()
			}
		}
	}()

	time.Sleep(cfg.warmup)
	before, err := snapshot(ctx, cfg)
	if err != nil {
		return err
	}
	measuring.Store(true)
	time.Sleep(cfg.duration)
	measuring.Store(false)
	after, err := snapshot(ctx, cfg)
	if err != nil {
		return err
	}
	stopLoad()
	wg.Wait()

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	pct := func(p float64) time.Duration {
		if len(latencies) == 0 {
			return 0
		}
		return latencies[int(float64(len(latencies)-1)*p)]
	}
	fmt.Fprintf(os.Stdout, "mode=%s rate=%d/s bulk=%d zipf=%s calls=%d errors=%d p50=%s p99=%s\n",
		cfg.mode, cfg.rate, cfg.bulk, flag.Lookup("zipf").Value, len(latencies), errs.Load(), pct(0.5), pct(0.99))
	report(os.Stdout, before, after, cfg.duration)
	return nil
}

func checkBulk(ctx context.Context, c *authzed.Client, cfg config, lw *lastWrite) error {
	items := make([]*v1.CheckBulkPermissionsRequestItem, 0, cfg.bulk)
	for range cfg.bulk {
		perm := "view"
		if rand.Intn(3) == 0 {
			perm = "edit"
		}
		items = append(items, &v1.CheckBulkPermissionsRequestItem{
			Resource:   &v1.ObjectReference{ObjectType: "resource", ObjectId: fmt.Sprint(zipfResource())},
			Permission: perm,
			Subject:    &v1.SubjectReference{Object: &v1.ObjectReference{ObjectType: "user", ObjectId: fmt.Sprint(zipfUser())}},
		})
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err := c.CheckBulkPermissions(ctx, &v1.CheckBulkPermissionsRequest{
		Consistency: lw.consistency(cfg.mode),
		Items:       items,
	})
	return err
}

func writeOne(ctx context.Context, c *authzed.Client, lw *lastWrite) error {
	resp, err := c.WriteRelationships(ctx, &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{{
			Operation:    v1.RelationshipUpdate_OPERATION_TOUCH,
			Relationship: rel("resource", fmt.Sprint(rand.Intn(resources)), "viewer", "user", fmt.Sprint(rand.Intn(users)), ""),
		}},
	})
	if err != nil {
		return fmt.Errorf("write: %w", err)
	}
	lw.token.Store(resp.WrittenAt)
	return nil
}

var (
	zipfNS   = rand.NewZipf(rand.New(rand.NewSource(1)), 1.1, 1, resources-1)
	zipfUsr  = rand.NewZipf(rand.New(rand.NewSource(2)), 1.1, 1, users-1)
	zipfLock sync.Mutex
)

func zipfResource() uint64 {
	zipfLock.Lock()
	defer zipfLock.Unlock()
	return zipfNS.Uint64()
}

func zipfUser() uint64 {
	zipfLock.Lock()
	defer zipfLock.Unlock()
	return zipfUsr.Uint64()
}
