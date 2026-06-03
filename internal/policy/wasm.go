package policy

import (
	"context"
	"fmt"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

// wasmPolicy runs a sandboxed WASM module authored in ANY language. The module
// exports:
//
//	score(weight f64, backlog i64, inflight i64, deficit f64, virtual_time f64, age_ms i64) f64
//
// called once per group. wazero gives a hard memory cap, a per-call CPU timeout
// (via context), and crash containment (a trap becomes an error → DRR fallback).
type wasmPolicy struct {
	rt    wazero.Runtime
	score api.Function
}

func compileWASM(code []byte) (Compiled, error) {
	ctx := context.Background()
	rt := wazero.NewRuntimeWithConfig(ctx, wazero.NewRuntimeConfig().
		WithCloseOnContextDone(true).
		WithMemoryLimitPages(4096)) // 256 MiB ceiling
	wasi_snapshot_preview1.MustInstantiate(ctx, rt)
	mod, err := rt.InstantiateWithConfig(ctx, code, wazero.NewModuleConfig().WithStartFunctions("_initialize"))
	if err != nil {
		_ = rt.Close(ctx)
		return nil, fmt.Errorf("wasm instantiate: %w", err)
	}
	fn := mod.ExportedFunction("score")
	if fn == nil {
		_ = rt.Close(ctx)
		return nil, fmt.Errorf("wasm module is missing the exported 'score' function")
	}
	return &wasmPolicy{rt: rt, score: fn}, nil
}

func (w *wasmPolicy) Score(l LaneView, _ ConsumerView) ([]float64, error) {
	out := make([]float64, len(l.Groups))
	for i, g := range l.Groups {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		res, err := w.score.Call(ctx,
			api.EncodeF64(g.Weight),
			uint64(int64(g.Backlog)),
			uint64(int64(g.InFlight)),
			api.EncodeF64(g.Deficit),
			api.EncodeF64(g.VirtualTime),
			uint64(g.AgeMs),
		)
		cancel()
		if err != nil {
			return nil, err
		}
		out[i] = api.DecodeF64(res[0])
	}
	return out, nil
}

func (w *wasmPolicy) Close() { _ = w.rt.Close(context.Background()) }
