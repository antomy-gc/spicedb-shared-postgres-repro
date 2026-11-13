package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

const simulatedWriteDuration = 5 * time.Millisecond

type background struct {
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	held           []*pgx.Conn
	openTxs        []pgx.Tx
	sdbWrites      atomic.Int64
	foreignCommits atomic.Int64
}

func startBackground(ctx context.Context, cfg config, p point) (*background, error) {
	bctx, cancel := context.WithCancel(ctx)
	b := &background{cancel: cancel}
	t0 := time.Now()
	fmt.Printf("   background: %d idle conns, %d open foreign txs, spicedb writes %d/s, foreign commits %d/s ...\n",
		p.Idle, p.Xip, p.SdbRate, p.ForeignRate)

	for range p.Idle {
		c, err := pgx.Connect(bctx, cfg.noiseDSN)
		if err != nil {
			b.stop()
			return nil, fmt.Errorf("opening idle connection: %w", err)
		}
		b.held = append(b.held, c)
	}
	for range p.Xip {
		c, err := pgx.Connect(bctx, cfg.noiseDSN)
		if err != nil {
			b.stop()
			return nil, fmt.Errorf("opening foreign transaction connection: %w", err)
		}
		b.held = append(b.held, c)
		tx, err := c.Begin(bctx)
		if err != nil {
			b.stop()
			return nil, fmt.Errorf("beginning foreign transaction: %w", err)
		}
		b.openTxs = append(b.openTxs, tx)
		if _, err := tx.Exec(bctx, `SELECT pg_current_xact_id()`); err != nil {
			b.stop()
			return nil, fmt.Errorf("assigning xid to foreign transaction: %w", err)
		}
	}

	sdbWrite := func(ctx context.Context, c *pgx.Conn) error {
		tx, err := c.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(context.Background()) }()
		if _, err := tx.Exec(ctx, `INSERT INTO relation_tuple_transaction (metadata) VALUES ('{}')`); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(simulatedWriteDuration):
		}
		return tx.Commit(ctx)
	}
	foreignCommit := func(ctx context.Context, c *pgx.Conn) error {
		_, err := c.Exec(ctx, `INSERT INTO noise_t DEFAULT VALUES`)
		return err
	}
	if err := b.startRate(bctx, cfg.sdbDSN, p.SdbRate, 32, sdbWrite, &b.sdbWrites); err != nil {
		b.stop()
		return nil, err
	}
	if err := b.startRate(bctx, cfg.noiseDSN, p.ForeignRate, 16, foreignCommit, &b.foreignCommits); err != nil {
		b.stop()
		return nil, err
	}

	fmt.Printf("   background ready in %s\n", time.Since(t0).Round(time.Millisecond))
	return b, nil
}

func (b *background) startRate(ctx context.Context, dsn string, rate, workers int, op func(context.Context, *pgx.Conn) error, counter *atomic.Int64) error {
	if rate <= 0 {
		return nil
	}
	workers = min(workers, rate)
	interval := time.Duration(float64(time.Second) * float64(workers) / float64(rate))
	for i := range workers {
		c, err := pgx.Connect(ctx, dsn)
		if err != nil {
			return fmt.Errorf("opening rate worker connection: %w", err)
		}
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			defer func() { _ = c.Close(context.Background()) }()
			select {
			case <-ctx.Done():
				return
			case <-time.After(interval * time.Duration(i) / time.Duration(workers)):
			}
			t := time.NewTicker(interval)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
				}
				if err := op(ctx, c); err != nil {
					if ctx.Err() == nil {
						fmt.Printf("   background worker stopped: %v\n", err)
					}
					return
				}
				counter.Add(1)
			}
		}()
	}
	return nil
}

func (b *background) stop() {
	b.cancel()
	b.wg.Wait()
	for _, tx := range b.openTxs {
		_ = tx.Rollback(context.Background())
	}
	for _, c := range b.held {
		_ = c.Close(context.Background())
	}
}

func journalRows(ctx context.Context, c *pgx.Conn) (int64, error) {
	var n int64
	err := c.QueryRow(ctx, `SELECT count(*) FROM relation_tuple_transaction`).Scan(&n)
	return n, err
}

func ensureJournal(ctx context.Context, c *pgx.Conn, target int) error {
	n, err := journalRows(ctx, c)
	if err != nil {
		return fmt.Errorf("counting journal rows: %w", err)
	}
	if n >= int64(target) {
		return nil
	}
	if err := appendJournal(ctx, c, target-int(n), "seeding journal"); err != nil {
		return err
	}
	fmt.Printf("   vacuum analyze relation_tuple_transaction ...\n")
	_, err = c.Exec(ctx, `VACUUM (ANALYZE) relation_tuple_transaction`, pgx.QueryExecModeSimpleProtocol)
	return err
}

func appendJournal(ctx context.Context, c *pgx.Conn, n int, label string) error {
	if _, err := c.Exec(ctx, `SET synchronous_commit = off`, pgx.QueryExecModeSimpleProtocol); err != nil {
		return err
	}
	defer func() {
		_, _ = c.Exec(context.Background(), `RESET synchronous_commit`, pgx.QueryExecModeSimpleProtocol)
	}()
	const chunk = 50_000
	t0 := time.Now()
	for done := 0; done < n; {
		k := min(chunk, n-done)
		sql := fmt.Sprintf(`DO $$ BEGIN FOR i IN 1..%d LOOP INSERT INTO relation_tuple_transaction (metadata) VALUES ('{}'); COMMIT; END LOOP; END $$`, k)
		if _, err := c.Exec(ctx, sql, pgx.QueryExecModeSimpleProtocol); err != nil {
			return fmt.Errorf("%s: %w", label, err)
		}
		done += k
		fmt.Printf("   %s: %d/%d rows (%.0f rows/s)\n", label, done, n, float64(done)/time.Since(t0).Seconds())
	}
	return nil
}
