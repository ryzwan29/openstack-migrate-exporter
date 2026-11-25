package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type migration struct {
	ID            int64  `json:"id"`
	UUID          string `json:"uuid"`
	InstanceUUID  string `json:"instance_uuid"`
	SourceCompute string `json:"source_compute"`
	DestCompute   string `json:"dest_compute"`
	Status        string `json:"status"`
	MigrationType string `json:"migration_type"`
	CreatedAt     string `json:"created_at"`
	UpdatedAt     string `json:"updated_at"`
}

func (m migration) key() string {
	if m.UUID != "" {
		return m.UUID
	}
	return strconv.FormatInt(m.ID, 10)
}

// progress is the in-flight data of GET /servers/{id}/migrations/{id} (live migration only).
type progress struct {
	MemoryTotal     *int64 `json:"memory_total_bytes"`
	MemoryProcessed *int64 `json:"memory_processed_bytes"`
	MemoryRemaining *int64 `json:"memory_remaining_bytes"`
	DiskTotal       *int64 `json:"disk_total_bytes"`
	DiskProcessed   *int64 `json:"disk_processed_bytes"`
	DiskRemaining   *int64 `json:"disk_remaining_bytes"`
}

type httpError struct {
	Code int
	Body string
}

func (e *httpError) Error() string { return fmt.Sprintf("HTTP %d: %s", e.Code, e.Body) }

type novaClient struct {
	ks           *keystone
	hc           *http.Client
	microversion string
	offsetNs     atomic.Int64 // estimated (nova clock - local clock)
}

// now returns the estimated Nova server time (migration timestamps use Nova's clock).
func (n *novaClient) now() time.Time { return time.Now().Add(time.Duration(n.offsetNs.Load())) }

func (n *novaClient) noteDate(h string) {
	t, err := http.ParseTime(h)
	if err != nil {
		return
	}
	d := time.Until(t) // Date header has 1s resolution, ignore small skew
	if d < 3*time.Second && d > -3*time.Second {
		d = 0
	}
	n.offsetNs.Store(int64(d))
}

func (n *novaClient) do(ctx context.Context, rawURL string, out any) error {
	for attempt := 0; ; attempt++ {
		tok, base, err := n.ks.get(ctx, attempt > 0)
		if err != nil {
			return err
		}
		full := rawURL
		if !strings.HasPrefix(rawURL, "http") {
			full = base + rawURL
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, full, nil)
		if err != nil {
			return err
		}
		req.Header.Set("X-Auth-Token", tok)
		req.Header.Set("Accept", "application/json")
		req.Header.Set("X-OpenStack-Nova-API-Version", n.microversion)
		resp, err := n.hc.Do(req)
		if err != nil {
			return err
		}
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
		resp.Body.Close()
		n.noteDate(resp.Header.Get("Date"))
		if resp.StatusCode == http.StatusUnauthorized && attempt == 0 {
			continue // token expired/revoked: re-auth once
		}
		if resp.StatusCode/100 != 2 {
			return &httpError{resp.StatusCode, snippet(data)}
		}
		return json.Unmarshal(data, out)
	}
}

// listMigrations follows pagination links.
func (n *novaClient) listMigrations(ctx context.Context, q url.Values) ([]migration, error) {
	var all []migration
	next := "/os-migrations?" + q.Encode()
	for page := 0; next != "" && page < 100; page++ {
		var r struct {
			Migrations []migration `json:"migrations"`
			Links      []struct {
				Rel  string `json:"rel"`
				Href string `json:"href"`
			} `json:"migrations_links"`
		}
		if err := n.do(ctx, next, &r); err != nil {
			return nil, err
		}
		all = append(all, r.Migrations...)
		next = ""
		for _, l := range r.Links {
			if l.Rel == "next" {
				next = l.Href
			}
		}
	}
	return all, nil
}

func (n *novaClient) getProgress(ctx context.Context, instance string, id int64) (*progress, error) {
	var r struct {
		Migration progress `json:"migration"`
	}
	if err := n.do(ctx, fmt.Sprintf("/servers/%s/migrations/%d", instance, id), &r); err != nil {
		return nil, err
	}
	return &r.Migration, nil
}

func parseNovaTime(s string) (time.Time, bool) {
	for _, layout := range []string{"2006-01-02T15:04:05.999999", "2006-01-02T15:04:05", time.RFC3339Nano, "2006-01-02 15:04:05"} {
		if t, err := time.ParseInLocation(layout, s, time.UTC); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}
