package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/Octavian-Mihai/shorter-url/internal/cache"
	"github.com/Octavian-Mihai/shorter-url/internal/config"
	"github.com/Octavian-Mihai/shorter-url/internal/events"
	"github.com/Octavian-Mihai/shorter-url/internal/events/kafka"
	"github.com/Octavian-Mihai/shorter-url/internal/httpapi"
	"github.com/Octavian-Mihai/shorter-url/internal/idgen"
	"github.com/Octavian-Mihai/shorter-url/internal/link"
	"github.com/Octavian-Mihai/shorter-url/internal/metrics"
	"github.com/Octavian-Mihai/shorter-url/internal/ratelimit"
	"github.com/Octavian-Mihai/shorter-url/internal/store/postgres"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	startCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	pool, err := postgres.Connect(startCtx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer rdb.Close()

	if err := kafka.EnsureTopic(startCtx, cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaPartitions, 1); err != nil {
		return err
	}

	keys := postgres.NewAPIKeyRepo(pool)
	if cfg.SeedAPIKey != "" {
		if err := keys.Seed(startCtx, "seed", cfg.SeedAPIKey); err != nil {
			return err
		}
	}

	reg := metrics.New()
	metrics.RegisterPool(reg, pool)

	alloc, err := idgen.NewAllocator(metrics.WrapBlockSource(reg, postgres.NewBlockSource(pool)), cfg.BlockSize)
	if err != nil {
		return err
	}
	gen := idgen.NewGenerator(alloc, idgen.NewScrambler(cfg.ScrambleSecret))

	selfHost := ""
	if u, err := url.Parse(cfg.BaseURL); err == nil {
		selfHost = u.Hostname()
	}
	links := link.NewService(postgres.NewLinkRepo(pool), cache.NewRedis(rdb), gen, link.Options{
		CacheTTL: cfg.CacheTTL, NegativeCacheTTL: cfg.NegativeCacheTTL, SelfHost: selfHost, Logger: log,
		Observer: metrics.LinkObserver(reg),
	})

	limiter, err := ratelimit.New(rdb, cfg.RateLimitBurst, cfg.RateLimitPerMinute)
	if err != nil {
		return err
	}
	clicks := events.NewAsync(kafka.NewPublisher(cfg.KafkaBrokers, cfg.KafkaTopic), events.AsyncOptions{Logger: log})
	metrics.RegisterAsync(reg, clicks)

	api := httpapi.New(httpapi.Deps{
		Links: links, Clicks: clicks, Auth: keys, Limiter: metrics.WrapLimiter(reg, limiter),
		Instrument: metrics.HTTPInstrument(reg), Stats: postgres.NewStatsRepo(pool),
		BaseURL: cfg.BaseURL, TrustProxy: cfg.TrustProxy, Logger: log,
		Readiness: map[string]func(context.Context) error{
			"postgres": func(ctx context.Context) error { return pool.Ping(ctx) },
			"redis":    func(ctx context.Context) error { return rdb.Ping(ctx).Err() },
		},
	})

	srv := &http.Server{
		Addr: cfg.HTTPAddr, Handler: api.Handler(),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second,
		WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second,
	}
	// Metrics live on a separate, unpublished port so they are never reachable
	// through the public load balancer.
	msrv := &http.Server{Addr: cfg.MetricsAddr, Handler: metricsMux(reg), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := msrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server", "err", err)
		}
	}()
	defer msrv.Close()

	errc := make(chan error, 1)
	go func() {
		log.Info("api listening", "addr", cfg.HTTPAddr)
		errc <- srv.ListenAndServe()
	}()

	select {
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
		log.Info("shutting down")
	}

	// Order matters: stop accepting requests, then flush queued click events,
	// then (via defers) close Redis and Postgres.
	shutCtx, cancelShut := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelShut()
	if err := srv.Shutdown(shutCtx); err != nil {
		log.Error("http shutdown", "err", err)
	}
	if err := clicks.Close(); err != nil {
		log.Error("flush clicks", "err", err)
	}
	log.Info("click queue flushed", "sent", clicks.Sent(), "dropped", clicks.Dropped(), "failed", clicks.Failed())
	return nil
}

func metricsMux(reg *prometheus.Registry) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", metrics.Handler(reg))
	return mux
}
