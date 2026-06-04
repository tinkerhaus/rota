// Command rotaviz is a live terminal visualization of Rota's fair scheduler.
// It runs a real in-process broker, keeps three weighted tenant groups topped up,
// drains them as fast as the broker commits, and renders the result while cycling
// the scheduling policy so you can SEE fairness change. Not committed; just a toy.
//
//	go run ./cmd/rotaviz            # live in your terminal (Ctrl-C to quit)
//	go run ./cmd/rotaviz | cat     # plain snapshots (non-TTY)
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/tinkerhaus/rota/internal/node"
	"github.com/tinkerhaus/rota/internal/policy"
)

const (
	lane        = "tenants"
	target      = 200 // keep each group's backlog near this
	feedLen     = 56
	policyEvery = 8 * time.Second
)

type grp struct {
	id     string
	weight float64
	color  string
}

var groups = []grp{
	{"A", 1, "32"}, // green
	{"B", 2, "36"}, // cyan
	{"C", 4, "33"}, // yellow
}

type policyStep struct {
	name string
	bind policy.Binding
	note string
}

var policies = []policyStep{
	{"Weighted Fair (WFQ)", policy.Binding{Kind: policy.KindWFQ}, "service ∝ weight (1:2:4), nobody starves"},
	{"Strict Priority", policy.Binding{Kind: policy.KindStrictPriority}, "highest weight wins → A and B starve"},
	{"Lottery", policy.Binding{Kind: policy.KindLottery}, "weighted-random pick each turn"},
	{"Shortest Queue (CEL: -backlog)", policy.Binding{Kind: policy.KindCEL, Source: []byte("-backlog")}, "custom policy ignores weight, equalizes queues"},
}

type state struct {
	mu        sync.Mutex
	published map[string]int
	servedAll map[string]int // lifetime, for backlog
	servedWin map[string]int // resets per policy, for the ratio bars
	feed      []string       // recent served group ids
	total     int
	polIdx    int
	useColor  bool
}

func main() {
	seconds := flag.Int("seconds", 0, "run for N seconds (0 = until Ctrl-C on a TTY, ~36s when piped)")
	flag.Parse()

	fi, _ := os.Stdout.Stat()
	isTTY := fi != nil && fi.Mode()&os.ModeCharDevice != 0

	dir, _ := os.MkdirTemp("", "rotaviz-*")
	defer os.RemoveAll(dir)
	n, err := node.Open(node.Config{DataDir: dir, NodeID: "viz", VisibilityMs: 60_000})
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(1)
	}
	defer n.Close()
	if err := n.WaitLeader(10 * time.Second); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	st := &state{
		published: map[string]int{}, servedAll: map[string]int{}, servedWin: map[string]int{},
		useColor: isTTY,
	}

	// Establish weights up front so they exist before the first publish.
	for _, g := range groups {
		w := g.weight
		_ = n.SetGroupConfig(lane, g.id, &w, nil)
	}
	_ = n.SetPolicy(lane, policies[0].bind)

	done := make(chan struct{})
	var once sync.Once
	stop := func() { once.Do(func() { close(done) }) }

	go publisher(n, st, done)
	go consumer(n, st, done)
	go policyCycler(n, st, done)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sig; stop() }()
	if *seconds > 0 {
		go func() { time.Sleep(time.Duration(*seconds) * time.Second); stop() }()
	}

	if isTTY {
		runTTY(st, done, stop)
	} else {
		runPlain(st, done, stop, *seconds)
	}
}

func publisher(n *node.Node, st *state, done chan struct{}) {
	for {
		select {
		case <-done:
			return
		default:
		}
		// Refill the group that is furthest below its target.
		st.mu.Lock()
		var pick string
		worst := -1
		for _, g := range groups {
			bl := st.published[g.id] - st.servedAll[g.id]
			if deficit := target - bl; deficit > worst {
				worst, pick = deficit, g.id
			}
		}
		if worst > 0 {
			st.published[pick]++
		}
		st.mu.Unlock()
		if worst <= 0 {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		if _, err := n.Publish(node.PublishReq{Lane: lane, GroupID: pick, Payload: []byte("x")}); err != nil {
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func consumer(n *node.Node, st *state, done chan struct{}) {
	for {
		select {
		case <-done:
			return
		default:
		}
		lr, ok, err := n.LeaseOne(lane, "viz")
		if err != nil || !ok {
			time.Sleep(3 * time.Millisecond)
			continue
		}
		_ = n.Ack(lr.LeaseID)
		st.mu.Lock()
		st.servedAll[lr.GroupID]++
		st.servedWin[lr.GroupID]++
		st.total++
		st.feed = append(st.feed, lr.GroupID)
		if len(st.feed) > feedLen {
			st.feed = st.feed[len(st.feed)-feedLen:]
		}
		st.mu.Unlock()
	}
}

func policyCycler(n *node.Node, st *state, done chan struct{}) {
	t := time.NewTicker(policyEvery)
	defer t.Stop()
	for {
		select {
		case <-done:
			return
		case <-t.C:
		}
		st.mu.Lock()
		st.polIdx = (st.polIdx + 1) % len(policies)
		idx := st.polIdx
		st.servedWin = map[string]int{} // fresh window per policy
		st.mu.Unlock()
		_ = n.SetPolicy(lane, policies[idx].bind)
	}
}

// ─── rendering ──────────────────────────────────────────────────────────────────

var (
	lastTotal int
	lastTime  = time.Now()
	rateEWMA  float64
	startTime = time.Now()
)

func (st *state) col(code, s string) string {
	if !st.useColor {
		return s
	}
	return "\033[" + code + "m" + s + "\033[0m"
}

func bar(n, width int) string {
	if n < 0 {
		n = 0
	}
	if n > width {
		n = width
	}
	return strings.Repeat("█", n) + strings.Repeat("░", width-n)
}

func (st *state) render() string {
	st.mu.Lock()
	defer st.mu.Unlock()

	now := time.Now()
	if dt := now.Sub(lastTime).Seconds(); dt > 0.2 {
		inst := float64(st.total-lastTotal) / dt
		if rateEWMA == 0 {
			rateEWMA = inst
		} else {
			rateEWMA = 0.6*rateEWMA + 0.4*inst
		}
		lastTotal, lastTime = st.total, now
	}

	maxWin := 1
	for _, g := range groups {
		if st.servedWin[g.id] > maxWin {
			maxWin = st.servedWin[g.id]
		}
	}

	pol := policies[st.polIdx]
	var b strings.Builder
	line := func(s string) { b.WriteString(s + clrEOL(st) + "\n") }

	line("")
	line("  " + st.col("1", "ROTA") + st.col("2", " · live fair-scheduling demo"))
	line("  " + strings.Repeat("─", 64))
	line("  policy: " + st.col("1;35", pol.name))
	line("  " + st.col("2", "        "+pol.note))
	line("  " + st.col("2", fmt.Sprintf("        auto-switches every %ds · elapsed %ds", int(policyEvery.Seconds()), int(now.Sub(startTime).Seconds()))))
	line("")
	line(st.col("2", "  group        backlog (queued)              served this policy"))
	for _, g := range groups {
		bl := st.published[g.id] - st.servedAll[g.id]
		win := st.servedWin[g.id]
		name := st.col(g.color, fmt.Sprintf("%s w%d", g.id, int(g.weight)))
		blBar := st.col(g.color, bar(bl*18/target, 18))
		winBar := st.col(g.color, bar(win*26/maxWin, 26))
		line(fmt.Sprintf("  %-14s %s %4d   %s %5d", name, blBar, bl, winBar, win))
	}
	line("")
	line("  recent: " + st.feedStr())
	line("")
	line(st.col("2", fmt.Sprintf("  throughput ~%3.0f msg/s · total served %d", rateEWMA, st.total)))
	line(st.col("2", "  Ctrl-C to quit"))
	line("")
	return b.String()
}

func (st *state) feedStr() string {
	color := map[string]string{}
	for _, g := range groups {
		color[g.id] = g.color
	}
	var sb strings.Builder
	for i := 0; i < feedLen-len(st.feed); i++ {
		sb.WriteString(" ")
	}
	for _, id := range st.feed {
		sb.WriteString(st.col(color[id], id))
	}
	return sb.String()
}

func clrEOL(st *state) string {
	if st.useColor {
		return "\033[K"
	}
	return ""
}

func runTTY(st *state, done chan struct{}, stop func()) {
	fmt.Print("\033[?25l\033[2J") // hide cursor, clear
	defer fmt.Print("\033[?25h\033[0m\n")
	tick := time.NewTicker(70 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-done:
			return
		case <-tick.C:
			fmt.Print("\033[H" + st.render() + "\033[0J")
		}
	}
}

func runPlain(st *state, done chan struct{}, stop func(), seconds int) {
	if seconds == 0 {
		go func() { time.Sleep(36 * time.Second); stop() }()
	}
	tick := time.NewTicker(1500 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-done:
			return
		case <-tick.C:
			fmt.Println(st.render())
			fmt.Println(strings.Repeat("=", 66))
		}
	}
}
