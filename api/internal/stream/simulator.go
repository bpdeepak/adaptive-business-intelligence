package stream

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"time"

	"abi/internal/model"
)

// SimConfig parameterizes the replay simulator.
type SimConfig struct {
	// Orders is the full historical order sequence, sorted by purchase time.
	Orders []model.ReplayOrder
	// Products is the sorted list of distinct products (for the price walk).
	Products []string
	// Price holds each product's current simulated price; the simulator walks
	// it forward across loops (state persists between loops).
	Price map[string]float64
	// Rate is simulated seconds per wall second (SPEED_MULTIPLIER).
	Rate float64
	// BotRatio is the fraction of sessions that are synthetic bots.
	BotRatio float64
	// Conversion is the fraction of sessions that convert (place an order).
	Conversion float64
	// ClickMin/ClickMax bound page views per session.
	ClickMin, ClickMax int
	// JitterMax caps the per-loop start offset (loop boundary jitter).
	JitterMax time.Duration
	// Now returns the current wall time (injectable for tests).
	Now func() time.Time
}

// Simulator replays the historical data as a continuous, looped event stream:
// each loop repeats the dataset (orders + converting sessions), abandoned
// sessions and catalog price changes are regenerated per simulated day with
// fresh randomness, and every loop gets a fresh loop-id so stored events never
// collide. Events land on a bounded Ring with drop-oldest backpressure.
type Simulator struct {
	cfg  SimConfig
	rng  *rand.Rand
	ring *Ring

	loopIdx    int
	loopOffset time.Duration
	loopDur    time.Duration
	clock      *Clock

	dayStart  time.Time
	dayCounts []int
	nDays     int

	sources [3]eventSource
	heads   [3]*pending
}

type pending struct {
	env Envelope
}

type eventSource interface {
	next(rng *rand.Rand) (Envelope, bool)
}

// NewSimulator builds a simulator. Pass a seeded rng for deterministic tests.
func NewSimulator(cfg SimConfig, rng *rand.Rand, ring *Ring) *Simulator {
	if cfg.ClickMin <= 0 {
		cfg.ClickMin = 3
	}
	if cfg.ClickMax <= cfg.ClickMin {
		cfg.ClickMax = cfg.ClickMin + 9
	}
	if cfg.JitterMax <= 0 {
		cfg.JitterMax = time.Hour
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}

	orders := append([]model.ReplayOrder(nil), cfg.Orders...)
	sort.SliceStable(orders, func(i, j int) bool {
		return orders[i].PurchaseAt.Before(orders[j].PurchaseAt)
	})
	cfg.Orders = orders

	s := &Simulator{cfg: cfg, rng: rng, ring: ring}
	if len(orders) > 0 {
		s.dayStart = startOfDay(orders[0].PurchaseAt)
		lastDay := startOfDay(orders[len(orders)-1].PurchaseAt)
		s.nDays = int(lastDay.Sub(s.dayStart).Hours()/24) + 1
	}
	s.dayCounts = make([]int, s.nDays)
	for _, o := range orders {
		s.dayCounts[int(startOfDay(o.PurchaseAt).Sub(s.dayStart).Hours()/24)]++
	}
	s.loopDur = s.loopSpan()
	s.loopIdx = 0
	s.loopOffset = 0
	s.clock = NewClock(s.dayStart, cfg.Rate)
	s.resetSources()
	return s
}

// loopSpan returns the simulated span of one full replay loop (≥ 1 day).
func (s *Simulator) loopSpan() time.Duration {
	if len(s.cfg.Orders) == 0 {
		return 24 * time.Hour
	}
	span := s.cfg.Orders[len(s.cfg.Orders)-1].PurchaseAt.Sub(s.cfg.Orders[0].PurchaseAt)
	if span < 24*time.Hour {
		span = 24 * time.Hour
	}
	return span
}

func startOfDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

func (s *Simulator) nextLoop() {
	s.loopIdx++
	jitter := time.Duration(s.rng.Int63n(int64(2*s.cfg.JitterMax))) - s.cfg.JitterMax
	s.loopOffset = time.Duration(s.loopIdx)*s.loopDur + jitter
	// The virtual clock is anchored ONCE at the dataset start and never moves:
	// elapsed wall time is monotonic across the whole run, and events are
	// shifted by loopOffset on the same timeline. Shifting the clock's base per
	// loop would slide simNow forward too, re-granting each new loop a fresh
	// "due" window and causing an unbounded wrap runaway.
	s.resetSources()
}

func (s *Simulator) resetSources() {
	ls := loopState{
		loopID:    fmt.Sprintf("l%d", s.loopIdx),
		offset:    s.loopOffset,
		dayStart:  s.dayStart,
		dayCounts: s.dayCounts,
		nDays:     s.nDays,
	}
	s.sources[0] = newOrderSource(ls, s.cfg)
	s.sources[1] = newAbandonSource(ls, s.cfg)
	s.sources[2] = newPriceSource(ls, s.cfg)
	for i := 0; i < 3; i++ {
		env, ok := s.sources[i].next(s.rng)
		if !ok {
			s.heads[i] = nil
			continue
		}
		s.heads[i] = &pending{env: env}
	}
}

// Advance emits every event whose simulated time is at or before the wall time
// `wall`, pushing them onto the ring. It returns how many events were emitted
// (0 = nothing due yet). This drives both the production loop and tests.
func (s *Simulator) Advance(wall time.Time) int {
	emitted := 0
	for {
		if s.emitOneDue(wall) {
			emitted++
			continue
		}
		if s.allExhausted() {
			s.nextLoop()
			if s.emitOneDue(wall) {
				emitted++
				continue
			}
		}
		return emitted
	}
}

// EmitAllDue is an alias kept for clarity in test code.
func (s *Simulator) EmitAllDue(wall time.Time) int { return s.Advance(wall) }

func (s *Simulator) emitOneDue(wall time.Time) bool {
	simNow := s.clock.Now(wall)
	idx := s.minHeadIdx()
	if idx < 0 {
		return false
	}
	h := s.heads[idx]
	if h.env.OccurredAt.After(simNow) {
		return false
	}
	s.ring.Push(h.env)
	env, ok := s.sources[idx].next(s.rng)
	if ok {
		s.heads[idx] = &pending{env: env}
	} else {
		s.heads[idx] = nil
	}
	return true
}

func (s *Simulator) minHeadIdx() int {
	best := -1
	for i := 0; i < 3; i++ {
		if s.heads[i] == nil {
			continue
		}
		if best == -1 || s.heads[i].env.OccurredAt.Before(s.heads[best].env.OccurredAt) {
			best = i
		}
	}
	return best
}

func (s *Simulator) allExhausted() bool {
	for i := 0; i < 3; i++ {
		if s.heads[i] != nil {
			return false
		}
	}
	return true
}

// Loop returns the current loop id (e.g. "l0").
func (s *Simulator) Loop() string { return fmt.Sprintf("l%d", s.loopIdx) }

// LoopIndex returns the current zero-based replay loop number.
func (s *Simulator) LoopIndex() int { return s.loopIdx }

// Rate returns the configured simulated-seconds-per-wall-second.
func (s *Simulator) Rate() float64 { return s.cfg.Rate }

// Run drives the simulator on the real wall clock until ctx is cancelled.
func (s *Simulator) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		if s.Advance(s.cfg.Now()) == 0 {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(20 * time.Millisecond):
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Event sources
// ---------------------------------------------------------------------------

// loopState is the per-loop context every source needs.
type loopState struct {
	loopID    string
	offset    time.Duration
	dayStart  time.Time
	dayCounts []int
	nDays     int
}

type subevent struct {
	env Envelope
}

// orderSource replays orders together with their converting sessions: a
// browsing funnel of page views, then the order, then (only for bot sessions)
// the ground-truth label. All events carry their simulated occurred time.
type orderSource struct {
	ls    loopState
	cfg   SimConfig
	idx   int
	queue []subevent
}

func newOrderSource(ls loopState, cfg SimConfig) *orderSource {
	return &orderSource{ls: ls, cfg: cfg}
}

func (o *orderSource) next(rng *rand.Rand) (Envelope, bool) {
	for len(o.queue) == 0 {
		if o.idx >= len(o.cfg.Orders) {
			return Envelope{}, false
		}
		o.buildQueue(rng, o.cfg.Orders[o.idx])
		o.idx++
	}
	e := o.queue[0]
	o.queue = o.queue[1:]
	return e.env, true
}

func (o *orderSource) buildQueue(rng *rand.Rand, od model.ReplayOrder) {
	orderTime := od.PurchaseAt.Add(o.ls.offset)
	sessID := newSessionID("c-"+od.CustomerID+"-"+od.OrderID, o.ls.loopID)
	isBot := rng.Float64() < o.cfg.BotRatio

	var queue []subevent
	var ss *sessionStats
	var lastEmitted time.Time
	if isBot {
		start := orderTime.Add(-250 * time.Millisecond)
		pages := botPages(rng, o.cfg.ClickMin+rng.Intn(o.cfg.ClickMax-o.cfg.ClickMin+1))
		ss = newSessionStats(start, true)
		last := start
		for i, page := range pages {
			at := start.Add(time.Duration(i) * 30 * time.Millisecond)
			ss.addPage(page, 0) // constant 30 ms burst → cv 0
			last = at
			queue = append(queue, subevent{
				env: newEnv(EventPageView, sessID, at, o.ls.loopID,
					o.pageView(sessID, page, od, rng))})
		}
		lastEmitted = last
		ss.setLast(last)
	} else {
		n := o.cfg.ClickMin + rng.Intn(o.cfg.ClickMax-o.cfg.ClickMin+1)
		pages := funnelPages(rng, n)
		start := orderTime.Add(-time.Duration(10+rng.Intn(30)) * time.Minute)
		ss = newSessionStats(start, true)
		t := start
		for _, page := range pages {
			gap := time.Duration(45+rng.Intn(300)) * time.Second
			t = t.Add(gap)
			// Stop once the walk is close to the order — but only after at
			// least one click has been emitted so sessions always browse.
			if len(queue) > 0 && t.After(orderTime.Add(-time.Duration(5+rng.Intn(30))*time.Second)) {
				break
			}
			ss.addPage(page, gap.Seconds())
			lastEmitted = t
			queue = append(queue, subevent{
				env: newEnv(EventPageView, sessID, t, o.ls.loopID,
					o.pageView(sessID, page, od, rng))})
		}
		// Corpus semantics: the session ends at the walk's final t — including
		// the overshoot gap when it broke early, and the dwell before click 1.
		ss.setLast(t)
	}

	queue = append(queue, subevent{
		env: newEnv(EventOrderPlaced, od.CustomerID, orderTime, o.ls.loopID,
			OrderPlaced{
				OrderID:      od.OrderID,
				CustomerID:   od.CustomerID,
				Status:       od.Status,
				PurchaseTime: orderTime,
				PaymentValue: od.PaymentValue,
				IsLost:       od.IsLost,
				Items:        toOrderItems(od.Items),
				Payments:     toOrderPayments(od.Payments),
			})})

	// Terminal marker for the completed converting session, placed just after
	// the last emitted click (scored regardless of bot/human).
	queue = append(queue, subevent{
		env: sessionEndEnv(ss, sessID, od.CustomerID, o.ls.loopID, lastEmitted.Add(time.Millisecond))})

	if isBot {
		queue = append(queue, subevent{
			env: newEnv(EventTrainingLabel, sessID, orderTime.Add(time.Millisecond), o.ls.loopID,
				SessionLabel{SessionID: sessID, IsBot: true, LoopID: o.ls.loopID, IsConverting: true})})
	}

	sort.SliceStable(queue, func(i, j int) bool { return queue[i].env.OccurredAt.Before(queue[j].env.OccurredAt) })
	o.queue = queue
}

func (o *orderSource) pageView(sessID, page string, od model.ReplayOrder, rng *rand.Rand) PageView {
	pid := ""
	if page == PageProduct && len(od.Items) > 0 {
		pid = od.Items[rng.Intn(len(od.Items))].ProductID
	}
	return PageView{SessionID: sessID, CustomerID: od.CustomerID, PageType: page, ProductID: pid}
}

func toOrderItems(items []model.ReplayItem) []OrderItem {
	out := make([]OrderItem, 0, len(items))
	for _, it := range items {
		out = append(out, OrderItem{
			ProductID: it.ProductID,
			SellerID:  it.SellerID,
			Price:     it.Price,
			Freight:   it.Freight,
			Quantity:  1,
		})
	}
	return out
}

func toOrderPayments(payments []model.ReplayPayment) []OrderPayment {
	out := make([]OrderPayment, 0, len(payments))
	for _, p := range payments {
		out = append(out, OrderPayment{
			Type:         p.Type,
			Installments: p.Installments,
			Value:        p.Value,
		})
	}
	return out
}

// ---------------------------------------------------------------------------
// session.end emission (Phase 3): every completed session — converting or
// abandoned, bot or human — ends with a SessionEnd event carrying the exact
// statistics the bot classifier needs, pre-aggregated here by the producer. The
// formulas mirror ml/replay_session_corpus.py (the training corpus) so the live
// feature space is the trained feature space: duration spans the dwell before
// the first click (and, on an early walk break, the overshoot gap), click_interval_cv
// is the population std (np.std, ddof=0) of the inter-click gaps (0 when <2
// gaps or mean <= 0), and hour_of_day/is_weekend come from the session start.
// ---------------------------------------------------------------------------

type sessionStats struct {
	startAt    time.Time
	lastAt     time.Time
	clickCount int
	gaps       []float64 // seconds, one per emitted page
	pageTypes  map[string]struct{}
	cartAdded  bool
	cartValue  float64
	converting bool
}

func newSessionStats(startAt time.Time, converting bool) *sessionStats {
	return &sessionStats{startAt: startAt, pageTypes: make(map[string]struct{}), converting: converting}
}

// addPage records one emitted page view and its trailing interval in seconds.
// A constant interval (bots) is recorded as 0 — the cv guard (mean <= 0) then
// reports 0.0 exactly like a constant-gap session.
func (s *sessionStats) addPage(page string, gapSeconds float64) {
	s.clickCount++
	s.gaps = append(s.gaps, gapSeconds)
	s.pageTypes[page] = struct{}{}
}

// setLast fixes the session's final timestamp: the end of the walk. On an early
// break this is the overshoot time (corpus `last = t`), otherwise the last
// emitted page's time.
func (s *sessionStats) setLast(t time.Time) { s.lastAt = t }

// build outputs the bot feature vector as the session.end payload.
func (s *sessionStats) build(sessID, customerID string) SessionEnd {
	flags := func(want string) int {
		if _, ok := s.pageTypes[want]; ok {
			return 1
		}
		return 0
	}
	return SessionEnd{
		SessionID:         sessID,
		CustomerID:        customerID,
		IsConverting:      b2i(s.converting),
		ClickCount:        s.clickCount,
		DurationSeconds:   round3(s.lastAt.Sub(s.startAt).Seconds()),
		ClickIntervalCV:   popStd(s.gaps),
		HasSearch:         flags(PageSearch),
		HasProductPage:    flags(PageProduct),
		HasCartPage:       flags(PageCart),
		HasCheckoutPage:   flags(PageCheckout),
		PageTypesDistinct: len(s.pageTypes),
		CartAdded:         b2i(s.cartAdded),
		CartValue:         round2f(s.cartValue),
		HourOfDay:         s.startAt.Hour(),
		IsWeekend:         b2i(s.startAt.Weekday() == time.Saturday || s.startAt.Weekday() == time.Sunday),
	}
}

func sessionEndEnv(ss *sessionStats, sessID, customerID, loopID string, at time.Time) Envelope {
	return newEnv(EventSessionEnd, sessID, at, loopID, ss.build(sessID, customerID))
}

// popStd is the population standard deviation (np.std semantics, ddof=0) with
// the corpus's cv guards: <2 gaps or a non-positive mean → 0.0.
func popStd(xs []float64) float64 {
	if len(xs) < 2 {
		return 0
	}
	sum := 0.0
	for _, x := range xs {
		sum += x
	}
	mean := sum / float64(len(xs))
	if mean <= 0 {
		return 0
	}
	v := 0.0
	for _, x := range xs {
		d := x - mean
		v += d * d
	}
	return math.Sqrt(v / float64(len(xs)))
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

// abandonSource generates abandoned sessions per simulated day, scaled to the
// day's order count and the configured conversion rate.
type abandonSource struct {
	ls    loopState
	cfg   SimConfig
	day   int
	queue []subevent
}

func newAbandonSource(ls loopState, cfg SimConfig) *abandonSource {
	return &abandonSource{ls: ls, cfg: cfg}
}

func (a *abandonSource) next(rng *rand.Rand) (Envelope, bool) {
	for len(a.queue) == 0 {
		if a.day >= a.ls.nDays {
			return Envelope{}, false
		}
		a.buildDay(rng, a.day)
		a.day++
	}
	e := a.queue[0]
	a.queue = a.queue[1:]
	return e.env, true
}

func (a *abandonSource) buildDay(rng *rand.Rand, dayIdx int) {
	dayStart := a.ls.dayStart.AddDate(0, 0, dayIdx).Add(a.ls.offset)
	dayEnd := dayStart.AddDate(0, 0, 1)
	span := dayEnd.Sub(dayStart)

	nAbandon := int(math.Round(float64(a.ls.dayCounts[dayIdx]) * (1 - a.cfg.Conversion) / a.cfg.Conversion))

	var queue []subevent
	for i := 0; i < nAbandon; i++ {
		t0 := dayStart.Add(time.Duration(rng.Float64() * float64(span)))
		sessID := newSessionID("a-"+randomHex(6), a.ls.loopID)
		isBot := rng.Float64() < a.cfg.BotRatio
		ss := newSessionStats(t0, false)

		var tTail time.Time
		if isBot {
			pages := botPages(rng, 1+rng.Intn(3))
			for v, page := range pages {
				at := t0.Add(time.Duration(v) * 40 * time.Millisecond)
				ss.addPage(page, 0) // constant 40 ms burst → cv 0
				queue = append(queue, subevent{
					env: newEnv(EventPageView, sessID, at, a.ls.loopID,
						a.pageView(sessID, page, rng))})
				tTail = at
			}
			ss.setLast(tTail)
		} else {
			pages := abandonPages(rng, 1+rng.Intn(3))
			t := t0
			for _, page := range pages {
				gapMin := 1 + rng.Intn(8)
				t = t.Add(time.Duration(gapMin) * time.Minute)
				ss.addPage(page, float64(gapMin)*60)
				queue = append(queue, subevent{
					env: newEnv(EventPageView, sessID, t, a.ls.loopID,
						a.pageView(sessID, page, rng))})
				tTail = t
			}
			ss.setLast(tTail)
		}

		if rng.Float64() < 0.6 {
			at := tTail.Add(time.Second)
			value := 0.0
			for it := 0; it < 1+rng.Intn(4); it++ {
				p := a.cfg.Products[rng.Intn(len(a.cfg.Products))]
				value += a.cfg.Price[p]
			}
			ss.cartAdded = true
			ss.cartValue = round2f(value)
			queue = append(queue, subevent{
				env: newEnv(EventCartAbandoned, sessID, at, a.ls.loopID,
					CartAbandoned{SessionID: sessID, ItemsCount: 1 + rng.Intn(4), CartValue: round2f(value)})})
		}

		// Terminal marker for the completed (abandoned) session, emitted after
		// any cart-abandon event and before the ground-truth label.
		queue = append(queue, subevent{
			env: sessionEndEnv(ss, sessID, "", a.ls.loopID, tTail.Add(time.Second+time.Millisecond))})

		if isBot {
			queue = append(queue, subevent{
				env: newEnv(EventTrainingLabel, sessID, tTail.Add(2*time.Second), a.ls.loopID,
					SessionLabel{SessionID: sessID, IsBot: true, LoopID: a.ls.loopID, IsConverting: false})})
		}
	}
	sort.SliceStable(queue, func(i, j int) bool { return queue[i].env.OccurredAt.Before(queue[j].env.OccurredAt) })
	a.queue = queue
}

func (a *abandonSource) pageView(sessID, page string, rng *rand.Rand) PageView {
	pid := ""
	if page == PageProduct && len(a.cfg.Products) > 0 {
		pid = a.cfg.Products[rng.Intn(len(a.cfg.Products))]
	}
	return PageView{SessionID: sessID, PageType: page, ProductID: pid}
}

// priceSource regenerates the catalog random walk: each product changes once
// per simulated week (±15%), i.e. ~1/7 of all products per day.
type priceSource struct {
	ls    loopState
	cfg   SimConfig
	day   int
	queue []subevent
}

func newPriceSource(ls loopState, cfg SimConfig) *priceSource {
	return &priceSource{ls: ls, cfg: cfg}
}

func (p *priceSource) next(rng *rand.Rand) (Envelope, bool) {
	for len(p.queue) == 0 {
		if p.day >= p.ls.nDays {
			return Envelope{}, false
		}
		p.buildDay(rng, p.day)
		p.day++
	}
	e := p.queue[0]
	p.queue = p.queue[1:]
	return e.env, true
}

func (p *priceSource) buildDay(rng *rand.Rand, dayIdx int) {
	dayStart := p.ls.dayStart.AddDate(0, 0, dayIdx).Add(p.ls.offset)
	dayEnd := dayStart.AddDate(0, 0, 1)
	span := dayEnd.Sub(dayStart)

	var queue []subevent
	for i, prod := range p.cfg.Products {
		if i%7 != dayIdx%7 {
			continue
		}
		old := p.cfg.Price[prod]
		if old <= 0 {
			old = 50
		}
		delta := 1 + (rng.Float64()*2-1)*0.15
		next := old * delta
		if next < 0.01 {
			next = 0.01
		}
		p.cfg.Price[prod] = next
		at := dayStart.Add(time.Duration(rng.Float64() * float64(span)))
		queue = append(queue, subevent{
			env: newEnv(EventPriceChanged, prod, at, p.ls.loopID,
				PriceChanged{ProductID: prod, OldPrice: round2f(old), NewPrice: round2f(next), ChangedAt: at})})
	}
	sort.SliceStable(queue, func(i, j int) bool { return queue[i].env.OccurredAt.Before(queue[j].env.OccurredAt) })
	p.queue = queue
}

// newEnv builds an envelope with a fully-resolved occurred time.
func newEnv(eventType, key string, at time.Time, loopID string, payload any) Envelope {
	env, err := NewEnvelope(eventType, key, at, loopID, payload)
	if err != nil {
		// Payload types are all concrete and marshalable; a failure here is a
		// programming error. Keep the empty envelope rather than panicking in
		// the hot path.
		return Envelope{EventType: eventType, OccurredAt: at, LoopID: loopID, Key: key}
	}
	return env
}

// ---------------------------------------------------------------------------
// Page-type helpers (deterministic per rng)
// ---------------------------------------------------------------------------

var funnel = []string{PageHome, PageCategory, PageSearch, PageProduct, PageCart, PageCheckout}

func funnelPages(rng *rand.Rand, n int) []string {
	out := make([]string, n)
	pos := -1
	for i := 0; i < n; i++ {
		pos += 1 + rng.Intn(2)
		if pos >= len(funnel) {
			pos = len(funnel) - 1
		}
		out[i] = funnel[pos]
	}
	return out
}

var browse = []string{PageHome, PageCategory, PageSearch, PageProduct, PageCart}

func abandonPages(rng *rand.Rand, n int) []string {
	out := make([]string, n)
	pos := 0
	for i := 0; i < n; i++ {
		pos += 1 + rng.Intn(len(browse)-1)
		if pos >= len(browse) {
			pos = len(browse) - 1
		}
		out[i] = browse[pos]
	}
	return out
}

func botPages(rng *rand.Rand, n int) []string {
	out := make([]string, n)
	for i := 0; i < n; i++ {
		switch i % 3 {
		case 0:
			out[i] = PageProduct
		case 1:
			out[i] = PageCart
		default:
			out[i] = PageCheckout
		}
	}
	return out
}

func round2f(v float64) float64 {
	return math.Round(v*100) / 100
}

// round3 rounds to 3 decimal places (the corpus's duration_seconds precision).
func round3(v float64) float64 {
	return math.Round(v*1000) / 1000
}
