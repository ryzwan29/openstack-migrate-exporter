package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

func newHTTPClient(cfg CloudConfig, insecure bool) (*http.Client, error) {
	tc := &tls.Config{InsecureSkipVerify: insecure || cfg.Insecure} //nolint:gosec // opt-in for lab clouds
	if cfg.CACert != "" {
		pem, err := os.ReadFile(cfg.CACert)
		if err != nil {
			return nil, fmt.Errorf("read cacert: %w", err)
		}
		pool, _ := x509.SystemCertPool()
		if pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("cacert %s: no certificates found", cfg.CACert)
		}
		tc.RootCAs = pool
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.TLSClientConfig = tc
	return &http.Client{Transport: tr, Timeout: 30 * time.Second}, nil
}

// keystone does Keystone v3 password auth (project scoped) and caches the token.
type keystone struct {
	cfg              CloudConfig
	hc               *http.Client
	endpointOverride string

	mu      sync.Mutex
	token   string
	expires time.Time
	compute string
}

func (k *keystone) tokensURL() string {
	u := strings.TrimRight(k.cfg.AuthURL, "/")
	if !strings.HasSuffix(u, "/v3") {
		u += "/v3"
	}
	return u + "/auth/tokens"
}

// get returns a valid token and the compute endpoint base URL.
func (k *keystone) get(ctx context.Context, force bool) (string, string, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if !force && k.token != "" && time.Until(k.expires) > time.Minute {
		return k.token, k.compute, nil
	}
	dom := func(name, id string) map[string]string {
		if id != "" {
			return map[string]string{"id": id}
		}
		if name == "" {
			name = "Default"
		}
		return map[string]string{"name": name}
	}
	user := map[string]any{"password": k.cfg.Password}
	if k.cfg.UserID != "" {
		user["id"] = k.cfg.UserID
	} else {
		user["name"] = k.cfg.Username
		user["domain"] = dom(k.cfg.UserDomainName, k.cfg.UserDomainID)
	}
	project := map[string]any{}
	if k.cfg.ProjectID != "" {
		project["id"] = k.cfg.ProjectID
	} else {
		project["name"] = k.cfg.ProjectName
		project["domain"] = dom(k.cfg.ProjectDomainName, k.cfg.ProjectDomainID)
	}
	body, _ := json.Marshal(map[string]any{"auth": map[string]any{
		"identity": map[string]any{"methods": []string{"password"}, "password": map[string]any{"user": user}},
		"scope":    map[string]any{"project": project},
	}})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, k.tokensURL(), bytes.NewReader(body))
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := k.hc.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("keystone: %w", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if resp.StatusCode != http.StatusCreated {
		return "", "", fmt.Errorf("keystone auth: HTTP %d: %s", resp.StatusCode, snippet(data))
	}
	var tr struct {
		Token struct {
			ExpiresAt string `json:"expires_at"`
			Catalog   []struct {
				Type      string `json:"type"`
				Endpoints []struct {
					Interface string `json:"interface"`
					URL       string `json:"url"`
					Region    string `json:"region"`
					RegionID  string `json:"region_id"`
				} `json:"endpoints"`
			} `json:"catalog"`
		} `json:"token"`
	}
	if err := json.Unmarshal(data, &tr); err != nil {
		return "", "", fmt.Errorf("keystone response: %w", err)
	}
	tok := resp.Header.Get("X-Subject-Token")
	if tok == "" {
		return "", "", fmt.Errorf("keystone: no X-Subject-Token header")
	}
	compute := strings.TrimRight(k.endpointOverride, "/")
	if compute == "" {
		want := strings.TrimSuffix(strings.ToLower(firstNonEmpty(k.cfg.Interface, "public")), "url")
		for _, svc := range tr.Token.Catalog {
			if svc.Type != "compute" {
				continue
			}
			for _, e := range svc.Endpoints {
				if strings.ToLower(e.Interface) != want {
					continue
				}
				if k.cfg.Region != "" && e.Region != k.cfg.Region && e.RegionID != k.cfg.Region {
					continue
				}
				compute = strings.TrimRight(e.URL, "/")
			}
		}
		if compute == "" {
			return "", "", fmt.Errorf("keystone: no %q compute endpoint in catalog (region %q); set -interface or -nova-endpoint", want, k.cfg.Region)
		}
	}
	exp, err := time.Parse(time.RFC3339, tr.Token.ExpiresAt)
	if err != nil {
		exp = time.Now().Add(30 * time.Minute)
	}
	k.token, k.expires, k.compute = tok, exp, compute
	return k.token, k.compute, nil
}

func snippet(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 300 {
		s = s[:300] + "..."
	}
	return s
}
