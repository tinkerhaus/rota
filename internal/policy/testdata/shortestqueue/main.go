// A WASM scheduling policy: "shortest queue first" — serve the group with the
// fewest leasable messages. Built for wasip1, exported for the host to call.
// (This is a test fixture; in production a policy could be authored in Rust,
// AssemblyScript, TinyGo, etc. — any language that compiles to a WASM reactor.)
package main

//go:wasmexport score
func score(weight float64, backlog int64, inflight int64, deficit float64, virtualTime float64, ageMs int64) float64 {
	return -float64(backlog)
}

func main() {}
