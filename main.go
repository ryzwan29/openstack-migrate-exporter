// openstack-migrate-exporter: Prometheus exporter for Nova live migrations.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

var version = "dev"

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func envDuration(k string, d time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if x, err := time.ParseDuration(v); err == nil {
			return x
		}
	}
	return d
}

func envBool(k string) bool {
	v := strings.ToLower(os.Getenv(k))
	return v == "1" || v == "true" || v == "yes"
}

func main() {
	var (
		listen      = flag.String("listen", envOr("EXPORTER_LISTEN", ":9181"), "listen address")
		cloudsFile  = flag.String("clouds", envOr("OS_CLIENT_CONFIG_FILE", "/etc/openstack/clouds.yaml"), "path to clouds.yaml")
		cloudName   = flag.String("cloud", os.Getenv("OS_CLOUD"), "cloud name in clouds.yaml (auto if the file has only one)")
		iface       = flag.String("interface", "", "endpoint interface: public|internal|admin (default: clouds.yaml, else public)")
		novaURL     = flag.String("nova-endpoint", os.Getenv("NOVA_ENDPOINT"), "override Nova endpoint, e.g. http://nova.example.com:8774/v2.1")
		microver    = flag.String("nova-microversion", envOr("NOVA_MICROVERSION", "2.80"), "Nova API microversion")
		interval    = flag.Duration("poll-interval", envDuration("POLL_INTERVAL", 5*time.Second), "Nova poll interval")
		lookback    = flag.Duration("lookback", envDuration("LOOKBACK", 168*time.Hour), "history window: ingested on startup and kept exported (per-migration series)")
		insecure    = flag.Bool("insecure", envBool("OS_INSECURE"), "skip TLS verification")
		healthcheck = flag.Bool("healthcheck", false, "probe /healthz of a running instance and exit")
		showVersion = flag.Bool("version", false, "print version and exit")
	)
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return
	}
	if *healthcheck {
		_, port, err := net.SplitHostPort(*listen)
		if err != nil {
			os.Exit(1)
		}
		c := http.Client{Timeout: 3 * time.Second}
		resp, err := c.Get("http://127.0.0.1:" + port + "/healthz")
		if err != nil || resp.StatusCode != http.StatusOK {
			os.Exit(1)
		}
		return
	}

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	cfg, err := loadCloud(*cloudsFile, *cloudName)
	if err != nil {
		log.Error("config", "err", err)
		os.Exit(1)
	}
	if *iface != "" {
		cfg.Interface = *iface
	}
	hc, err := newHTTPClient(cfg, *insecure)
	if err != nil {
		log.Error("http client", "err", err)
		os.Exit(1)
	}
	ks := &keystone{cfg: cfg, hc: hc, endpointOverride: *novaURL}
	nova := &novaClient{ks: ks, hc: hc, microversion: *microver}
	p := newPoller(nova, *interval, *lookback, log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go p.run(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		p.writeMetrics(w)
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if p.healthy() {
			fmt.Fprintln(w, "ok")
			return
		}
		http.Error(w, "nova polling failing", http.StatusServiceUnavailable)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		fmt.Fprintf(w, "openstack-migrate-exporter %s\n<a href=\"/metrics\">/metrics</a>\n", version)
	})
	srv := &http.Server{Addr: *listen, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(c)
	}()
	log.Info("starting", "version", version, "listen", *listen, "auth_url", cfg.AuthURL, "interval", *interval)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("server", "err", err)
		os.Exit(1)
	}
}