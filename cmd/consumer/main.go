package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Octavian-Mihai/shorter-url/internal/config"
	"github.com/Octavian-Mihai/shorter-url/internal/consumer"
	"github.com/Octavian-Mihai/shorter-url/internal/events/kafka"
	"github.com/Octavian-Mihai/shorter-url/internal/metrics"
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
	if err := kafka.EnsureTopic(startCtx, cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaPartitions, 1); err != nil {
		return err
	}

	src := consumer.NewKafkaSource(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroup)
	defer src.Close()

	reg := metrics.New()
	metrics.RegisterPool(reg, pool)
	metrics.RegisterLag(reg, src.Lag)
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", metrics.Handler(reg))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	msrv := &http.Server{Addr: cfg.MetricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := msrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server", "err", err)
		}
	}()
	defer msrv.Close()

	log.Info("consumer started", "topic", cfg.KafkaTopic, "group", cfg.KafkaGroup,
		"batch", cfg.ConsumerBatchSize, "flush_every", cfg.ConsumerFlushEvery)
	c := consumer.New(src, postgres.NewClickSink(pool), consumer.Options{
		BatchSize: cfg.ConsumerBatchSize, FlushEvery: cfg.ConsumerFlushEvery, Logger: log,
		Observer: metrics.ConsumerObserver(reg),
	})
	err = c.Run(ctx)
	log.Info("consumer stopped")
	return err
}
