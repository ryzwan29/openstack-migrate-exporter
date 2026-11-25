package main

import (
	"context"
	"log/slog"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

var durationBuckets = []float64{5, 10, 20, 30, 45, 60, 90, 120, 180, 300, 600, 1200, 1800, 3600}

var terminalStatuses = map[string]bool{
	"completed": true, "failed": true, "error": true, "cancelled": true,
	"done": true, "confirmed": true, "reverted": true, "finished": true,
}

type pairKey struct{ source, dest, status string }

type hist struct {
	buckets []uint64 // non-cumulative per bucket, last = +Inf
	sum     float64
	count   uint64
}

func (h *hist) observe(v float64) {
	i := sort.SearchFloat64s(durationBuckets, v) // first bucket >= v
	h.buckets[i]++
	h.sum += v
	h.count++
}

type lastInfo struct {
	duration  float64
	finished  float64 // unix seconds
	processed float64 // last observed memory_processed_bytes (0 = unknown)
	avgSpeed  float64 // processed / duration (0 = unknown)
}

type inflight struct {
	key, instance, source, dest, status string
	id                                  int64
	created                             time.Time
	prog                                *progress
	lastProcessed                       int64
	lastAt                              time.Time
	haveLast                            bool
	speed                               float64
	haveSpeed                           bool
	misses                              int
}

// Poller polls Nova and keeps the state exported on /metrics.
type Poller struct {
	nova     *novaClient
	interval time.Duration
	lookback time.Duration
	log      *slog.Logger

	mu       sync.Mutex
	cursor   time.Time
	inflight map[string]*inflight
	finished map[string]time.Time
	counts   map[pairKey]uint64
	hists    map[pairKey]*hist
	last     map[pairKey]*lastInfo

	up       bool
	lastOK   time.Time
	lastPoll time.Time
	pollDur  float64
	errs     uint64
}

func newPoller(n *novaClient, interval, lookback time.Duration, log *slog.Logger) *Poller {
	return &Poller{
		nova: n, interval: interval, lookback: lookback, log: log,
		inflight: map[string]*inflight{}, finished: map[string]time.Time{},
		counts: map[pairKey]uint64{}, hists: map[pairKey]*hist{}, last: map[pairKey]*lastInfo{},
	}
}

func (p *Poller) run(ctx context.Context) {
	tick := func() {
		c, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()
		t0 := time.Now()
		err := p.poll(c)
		p.mu.Lock()
		p.lastPoll = time.Now()
		p.pollDur = time.Since(t0).Seconds()
		if err != nil {
			p.up = false
			p.errs++
		} else {
			p.up = true
			p.lastOK = time.Now()
		}
		p.mu.Unlock()
		if err != nil && ctx.Err() == nil {
			p.log.Error("poll failed", "err", err)
		}
	}
	tick()
	t := time.NewTicker(p.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			tick()
		}
	}
}

func (p *Poller) healthy() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	limit := 5 * p.interval
	if limit < time.Minute {
		limit = time.Minute
	}
	return p.up && time.Since(p.lastOK) < limit
}

func (p *Poller) poll(ctx context.Context) error {
	now := p.nova.now()
	p.mu.Lock()
	cursor := p.cursor
	p.mu.Unlock()
	if cursor.IsZero() {
		cursor = now.Add(-p.lookback)
	}
	base := func() url.Values {
		return url.Values{"migration_type": {"live-migration"}, "limit": {"500"}}
	}

	q := base()
	q.Set("changes-since", cursor.UTC().Format(time.RFC3339))
	changed, err := p.nova.listMigrations(ctx, q)
	if err != nil {
		return err
	}
	q = base()
	q.Set("status", "running")
	running, err := p.nova.listMigrations(ctx, q)
	if err != nil {
		return err
	}
	seen := map[string]migration{}
	for _, m := range append(changed, running...) {
		if m.MigrationType != "" && m.MigrationType != "live-migration" {
			continue
		}
		seen[m.key()] = m
	}

	// tracked in-flight migrations that did not show up: look them up by instance
	p.mu.Lock()
	var missing []string
	for k, in := range p.inflight {
		if _, ok := seen[k]; !ok {
			missing = append(missing, in.instance)
		}
	}
	p.mu.Unlock()
	for _, inst := range missing {
		q = base()
		q.Set("instance_uuid", inst)
		ms, err := p.nova.listMigrations(ctx, q)
		if err != nil {
			return err
		}
		for _, m := range ms {
			if _, ok := p.inflightGet(m.key()); ok {
				seen[m.key()] = m
			}
		}
	}

	list := make([]migration, 0, len(seen))
	for _, m := range seen {
		list = append(list, m)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].CreatedAt < list[j].CreatedAt })

	p.mu.Lock()
	p.apply(list, seen, now)
	snap := make([]*inflight, 0, len(p.inflight))
	for _, in := range p.inflight {
		c := *in
		snap = append(snap, &c)
	}
	p.mu.Unlock()

	// progress of running migrations (outside the lock)
	type res struct {
		key string
		pr  *progress
	}
	var results []res
	for _, in := range snap {
		pr, err := p.nova.getProgress(ctx, in.instance, in.id)
		if err != nil {
			if he, ok := err.(*httpError); ok && he.Code == 404 {
				continue // not (yet) a running live migration
			}
			p.log.Warn("progress fetch failed", "migration", in.key, "err", err)
			continue
		}
		results = append(results, res{in.key, pr})
	}
	tnow := time.Now()
	p.mu.Lock()
	for _, r := range results {
		in := p.inflight[r.key]
		if in == nil {
			continue
		}
		in.prog = r.pr
		if r.pr.MemoryProcessed != nil {
			cur := *r.pr.MemoryProcessed
			if in.haveLast && tnow.After(in.lastAt) && cur >= in.lastProcessed {
				in.speed = float64(cur-in.lastProcessed) / tnow.Sub(in.lastAt).Seconds()
				in.haveSpeed = true
			}
			in.lastProcessed, in.lastAt, in.haveLast = cur, tnow, true
		}
	}
	overlap := 2 * p.interval
	if overlap < time.Minute {
		overlap = time.Minute
	}
	p.cursor = now.Add(-overlap)
	// prune dedupe map
	for k, t := range p.finished {
		if t.Before(p.cursor.Add(-time.Hour)) {
			delete(p.finished, k)
		}
	}
	p.mu.Unlock()
	return nil
}

func (p *Poller) inflightGet(k string) (*inflight, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	in, ok := p.inflight[k]
	return in, ok
}

// apply must be called with p.mu held.
func (p *Poller) apply(list []migration, seen map[string]migration, now time.Time) {
	for _, m := range list {
		k := m.key()
		status := strings.ToLower(m.Status)
		created, _ := parseNovaTime(m.CreatedAt)
		if terminalStatuses[status] {
			if _, done := p.finished[k]; done {
				continue
			}
			updated, ok := parseNovaTime(m.UpdatedAt)
			if !ok {
				updated = now
			}
			p.finished[k] = updated
			dur := updated.Sub(created).Seconds()
			if created.IsZero() || dur < 0 {
				dur = 0
			}
			pk := pairKey{m.SourceCompute, m.DestCompute, status}
			p.counts[pk]++
			h := p.hists[pk]
			if h == nil {
				h = &hist{buckets: make([]uint64, len(durationBuckets)+1)}
				p.hists[pk] = h
			}
			h.observe(dur)
			li := &lastInfo{duration: dur, finished: float64(updated.Unix())}
			if in := p.inflight[k]; in != nil && in.haveLast && in.lastProcessed > 0 {
				li.processed = float64(in.lastProcessed)
				if dur > 0 {
					li.avgSpeed = li.processed / dur
				}
			}
			p.last[pk] = li
			delete(p.inflight, k)
			continue
		}
		in := p.inflight[k]
		if in == nil {
			in = &inflight{key: k}
			p.inflight[k] = in
		}
		in.id, in.instance = m.ID, m.InstanceUUID
		in.source, in.dest, in.status = m.SourceCompute, m.DestCompute, status
		in.created = created
		in.misses = 0
	}
	for k, in := range p.inflight {
		if _, ok := seen[k]; !ok {
			in.misses++
			if in.misses >= 3 {
				delete(p.inflight, k)
			}
		}
	}
}
