// Package observe holds Rota's Prometheus metrics. They are package-level so any
// component can increment them; the binary exposes them on /metrics.
package observe

import "github.com/prometheus/client_golang/prometheus"

var (
	Publishes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "rota_publishes_total", Help: "Messages published.",
	})
	Leases = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "rota_leases_total", Help: "Messages leased to consumers.",
	})
	Acks = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "rota_acks_total", Help: "Messages acknowledged.",
	})
	Nacks = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "rota_nacks_total", Help: "Negative acknowledgements by mode.",
	}, []string{"mode"})
	DeadLetters = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "rota_dead_letters_total", Help: "Messages routed to the dead-letter queue.",
	})
	TimerFires = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "rota_timer_fires_total", Help: "Time-index entries fired, by kind.",
	}, []string{"kind"})
	PolicyFaults = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "rota_policy_faults_total", Help: "Policy evaluations that faulted and fell back to WFQ.",
	})
)

func init() {
	prometheus.MustRegister(Publishes, Leases, Acks, Nacks, DeadLetters, TimerFires, PolicyFaults)
}
