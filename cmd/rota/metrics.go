package main

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/tinkerhaus/rota/internal/node"
)

func registerNodeHealthMetrics(n *node.Node) {
	prometheus.MustRegister(
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "rota_node_serving",
			Help: "1 when the node reports serving.",
		}, func() float64 {
			serving, _, _ := n.Health()
			return boolGauge(serving)
		}),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "rota_cluster_has_quorum",
			Help: "1 when the node sees a current Raft leader/quorum.",
		}, func() float64 {
			_, quorum, _ := n.Health()
			return boolGauge(quorum)
		}),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "rota_node_is_leader",
			Help: "1 when this node is the current Raft leader.",
		}, func() float64 {
			_, _, leader := n.Health()
			return boolGauge(leader)
		}),
	)
}

func boolGauge(v bool) float64 {
	if v {
		return 1
	}
	return 0
}
