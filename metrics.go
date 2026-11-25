package main

import (
	"bufio"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
)

type mwriter struct{ w *bufio.Writer }

func (m mwriter) family(name, typ, help string) {
	m.w.WriteString("# HELP " + name + " " + help + "\n")
	m.w.WriteString("# TYPE " + name + " " + typ + "\n")
}

func (m mwriter) sample(name string, v float64, labels ...string) {
	m.w.WriteString(name)
	if len(labels) > 0 {
		m.w.WriteByte('{')
		for i := 0; i+1 < len(labels); i += 2 {
			if i > 0 {
				m.w.WriteByte(',')
			}
			m.w.WriteString(labels[i] + `="` + escLabel(labels[i+1]) + `"`)
		}
		m.w.WriteByte('}')
	}
	m.w.WriteByte(' ')
	switch {
	case math.IsInf(v, 1):
		m.w.WriteString("+Inf")
	case math.IsNaN(v):
		m.w.WriteString("NaN")
	default:
		m.w.WriteString(strconv.FormatFloat(v, 'g', -1, 64))
	}
	m.w.WriteByte('\n')
}

func escLabel(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
	return r.Replace(s)
}

func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

func sortedPairs[T any](m map[pairKey]T) []pairKey {
	keys := make([]pairKey, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		if a.source != b.source {
			return a.source < b.source
		}
		if a.dest != b.dest {
			return a.dest < b.dest
		}
		return a.status < b.status
	})
	return keys
}

func (p *Poller) writeMetrics(out io.Writer) {
	bw := bufio.NewWriter(out)
	defer bw.Flush()
	m := mwriter{bw}
	now := p.nova.now()

	p.mu.Lock()
	defer p.mu.Unlock()

	m.family("nova_migrate_exporter_build_info", "gauge", "Build information.")
	m.sample("nova_migrate_exporter_build_info", 1, "version", version)
	m.family("nova_migrate_exporter_up", "gauge", "1 if the last poll of the Nova API succeeded.")
	m.sample("nova_migrate_exporter_up", b2f(p.up))
	m.family("nova_migrate_exporter_poll_errors_total", "counter", "Failed polls of the Nova API.")
	m.sample("nova_migrate_exporter_poll_errors_total", float64(p.errs))
	m.family("nova_migrate_exporter_poll_duration_seconds", "gauge", "Duration of the last poll.")
	m.sample("nova_migrate_exporter_poll_duration_seconds", p.pollDur)
	m.family("nova_migrate_exporter_last_success_timestamp_seconds", "gauge", "Unix time of the last successful poll.")
	if !p.lastOK.IsZero() {
		m.sample("nova_migrate_exporter_last_success_timestamp_seconds", float64(p.lastOK.Unix()))
	} else {
		m.sample("nova_migrate_exporter_last_success_timestamp_seconds", 0)
	}

	// ---- in-flight ----
	keys := make([]string, 0, len(p.inflight))
	for k := range p.inflight {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	m.family("nova_live_migration_in_progress", "gauge", "Number of live migrations currently in progress.")
	m.sample("nova_live_migration_in_progress", float64(len(keys)))

	lbl := func(in *inflight) []string {
		return []string{"migration", in.key, "instance", in.instance, "source", in.source, "dest", in.dest}
	}
	m.family("nova_live_migration_info", "gauge", "In-progress live migration (value 1), with its Nova status.")
	for _, k := range keys {
		in := p.inflight[k]
		m.sample("nova_live_migration_info", 1, append(lbl(in), "status", in.status)...)
	}
	m.family("nova_live_migration_elapsed_seconds", "gauge", "Seconds since the in-progress live migration was created.")
	for _, k := range keys {
		in := p.inflight[k]
		if in.created.IsZero() {
			continue
		}
		e := now.Sub(in.created).Seconds()
		if e < 0 {
			e = 0
		}
		m.sample("nova_live_migration_elapsed_seconds", e, lbl(in)...)
	}
	type fld struct {
		name, help string
		get        func(*progress) *int64
	}
	for _, f := range []fld{
		{"nova_live_migration_memory_total_bytes", "Total guest memory to migrate.", func(x *progress) *int64 { return x.MemoryTotal }},
		{"nova_live_migration_memory_processed_bytes", "Memory transferred so far (includes re-sent dirty pages).", func(x *progress) *int64 { return x.MemoryProcessed }},
		{"nova_live_migration_memory_remaining_bytes", "Memory still to transfer.", func(x *progress) *int64 { return x.MemoryRemaining }},
		{"nova_live_migration_disk_total_bytes", "Total disk to migrate (block migration).", func(x *progress) *int64 { return x.DiskTotal }},
		{"nova_live_migration_disk_processed_bytes", "Disk transferred so far.", func(x *progress) *int64 { return x.DiskProcessed }},
		{"nova_live_migration_disk_remaining_bytes", "Disk still to transfer.", func(x *progress) *int64 { return x.DiskRemaining }},
	} {
		m.family(f.name, "gauge", f.help)
		for _, k := range keys {
			in := p.inflight[k]
			if in.prog == nil {
				continue
			}
			if v := f.get(in.prog); v != nil {
				m.sample(f.name, float64(*v), lbl(in)...)
			}
		}
	}
	m.family("nova_live_migration_memory_progress_ratio", "gauge", "1 - memory_remaining/memory_total (0..1). Can move backwards when the guest dirties memory faster than it is copied.")
	for _, k := range keys {
		in := p.inflight[k]
		if in.prog == nil || in.prog.MemoryTotal == nil || in.prog.MemoryRemaining == nil || *in.prog.MemoryTotal <= 0 {
			continue
		}
		r := 1 - float64(*in.prog.MemoryRemaining)/float64(*in.prog.MemoryTotal)
		m.sample("nova_live_migration_memory_progress_ratio", math.Max(0, math.Min(1, r)), lbl(in)...)
	}
	m.family("nova_live_migration_memory_speed_bytes_per_second", "gauge", "Memory transfer speed between the last two polls.")
	for _, k := range keys {
		in := p.inflight[k]
		if in.haveSpeed {
			m.sample("nova_live_migration_memory_speed_bytes_per_second", in.speed, lbl(in)...)
		}
	}
	m.family("nova_live_migration_memory_avg_speed_bytes_per_second", "gauge", "Average memory transfer speed since the migration was created.")
	for _, k := range keys {
		in := p.inflight[k]
		if in.prog == nil || in.prog.MemoryProcessed == nil || in.created.IsZero() {
			continue
		}
		if e := now.Sub(in.created).Seconds(); e > 0 {
			m.sample("nova_live_migration_memory_avg_speed_bytes_per_second", float64(*in.prog.MemoryProcessed)/e, lbl(in)...)
		}
	}

	// ---- finished ----
	m.family("nova_live_migrations_total", "counter", "Finished live migrations seen by the exporter, by outcome.")
	for _, k := range sortedPairs(p.counts) {
		m.sample("nova_live_migrations_total", float64(p.counts[k]), "source", k.source, "dest", k.dest, "status", k.status)
	}
	m.family("nova_live_migration_duration_seconds", "histogram", "Duration (updated_at - created_at) of finished live migrations.")
	for _, k := range sortedPairs(p.hists) {
		h := p.hists[k]
		var cum uint64
		for i, le := range durationBuckets {
			cum += h.buckets[i]
			m.sample("nova_live_migration_duration_seconds_bucket", float64(cum), "source", k.source, "dest", k.dest, "status", k.status, "le", strconv.FormatFloat(le, 'g', -1, 64))
		}
		m.sample("nova_live_migration_duration_seconds_bucket", float64(h.count), "source", k.source, "dest", k.dest, "status", k.status, "le", "+Inf")
		m.sample("nova_live_migration_duration_seconds_sum", h.sum, "source", k.source, "dest", k.dest, "status", k.status)
		m.sample("nova_live_migration_duration_seconds_count", float64(h.count), "source", k.source, "dest", k.dest, "status", k.status)
	}
	pairs := sortedPairs(p.last)
	m.family("nova_live_migration_last_duration_seconds", "gauge", "Duration of the most recent finished live migration per source/dest/status.")
	for _, k := range pairs {
		m.sample("nova_live_migration_last_duration_seconds", p.last[k].duration, "source", k.source, "dest", k.dest, "status", k.status)
	}
	m.family("nova_live_migration_last_finished_timestamp_seconds", "gauge", "Unix time the most recent live migration finished.")
	for _, k := range pairs {
		m.sample("nova_live_migration_last_finished_timestamp_seconds", p.last[k].finished, "source", k.source, "dest", k.dest, "status", k.status)
	}
	m.family("nova_live_migration_last_memory_processed_bytes", "gauge", "Last observed memory_processed_bytes of the most recent migration (only if it was seen in flight).")
	for _, k := range pairs {
		if v := p.last[k].processed; v > 0 {
			m.sample("nova_live_migration_last_memory_processed_bytes", v, "source", k.source, "dest", k.dest, "status", k.status)
		}
	}
	m.family("nova_live_migration_last_avg_speed_bytes_per_second", "gauge", "Average memory speed of the most recent migration (only if it was seen in flight).")
	for _, k := range pairs {
		if v := p.last[k].avgSpeed; v > 0 {
			m.sample("nova_live_migration_last_avg_speed_bytes_per_second", v, "source", k.source, "dest", k.dest, "status", k.status)
		}
	}
}
