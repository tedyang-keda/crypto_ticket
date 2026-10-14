package guardian

import (
	"context"
	"sync"
)

// runTargetPool bounds symbol-level work for one exchange. REST calls still
// pass through the shared exchange semaphore and pacer, so other Guardian
// paths cannot bypass either limit.
func (g *Guardian) runTargetPool(ctx context.Context, targets []auditTarget, work func(context.Context, auditTarget)) {
	if len(targets) == 0 || ctx.Err() != nil {
		return
	}
	workers := minInt(g.cfg.MaxInflightPerExchange, len(targets))
	if workers < 1 {
		workers = 1
	}

	jobs := make(chan auditTarget)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for target := range jobs {
				if ctx.Err() != nil {
					continue
				}
				work(ctx, target)
			}
		}()
	}

sendTargets:
	for _, target := range targets {
		select {
		case <-ctx.Done():
			break sendTargets
		case jobs <- target:
		}
	}
	close(jobs)
	wg.Wait()
}
