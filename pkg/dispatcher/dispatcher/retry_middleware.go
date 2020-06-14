package dispatcher

import (
	"context"
	"github.com/slok/goresilience"
	"github.com/slok/goresilience/metrics"
	"github.com/slok/goresilience/retry"
	"math"
	"math/rand"
	"time"
)

type Retries struct {
	times int
}

func NewMiddleware(cfg retry.Config) goresilience.Middleware {
	if cfg.WaitBase <= 0 {
		cfg.WaitBase = 20 * time.Millisecond
	}

	if cfg.Times <= 0 {
		cfg.Times = 3
	}

	return func(next goresilience.Runner) goresilience.Runner {
		next = goresilience.SanitizeRunner(next)

		return goresilience.RunnerFunc(func(ctx context.Context, f goresilience.Func) error {
			var err error
			metricsRecorder, _ := metrics.RecorderFromContext(ctx)

			for i := 0; i <= cfg.Times; i++ {

				if i != 0 {
					metricsRecorder.IncRetry()
				}

				funcCtx := context.WithValue(ctx, "retries", Retries{
					times: i,
				})

				err = next.Run(funcCtx, f)
				if err == nil {
					return nil
				}

				waitDuration := cfg.WaitBase

				if !cfg.DisableBackoff {
					exp := math.Exp2(float64(i + 1))
					waitDuration = time.Duration(float64(cfg.WaitBase) * exp)
					// Round to millisecs.
					waitDuration = waitDuration.Round(time.Millisecond)

					// Apply "full jitter".
					random := rand.New(rand.NewSource(time.Now().UnixNano()))
					waitDuration = time.Duration(float64(waitDuration) * random.Float64())
				}

				time.Sleep(waitDuration)
			}

			return err
		})
	}
}