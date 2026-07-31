// Copyright (c) 2021 VMware, Inc. or its affiliates. All Rights Reserved.
// Copyright (c) 2012-2021, Sean Treadway, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package amqp091

import (
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// settleGoroutines waits until runtime.NumGoroutine() stops changing across
// successive samples (or a deadline elapses) and returns the settled count.
// Used by the goroutine-budget assertions below, which compare relative
// deltas rather than absolute counts so unrelated runtime goroutines do not
// make the test flaky.
func settleGoroutines() int {
	deadline := time.Now().Add(2 * time.Second)
	prev := -1
	stable := 0
	for {
		runtime.Gosched()
		time.Sleep(5 * time.Millisecond)
		n := runtime.NumGoroutine()
		if n == prev {
			stable++
			if stable >= 3 || time.Now().After(deadline) {
				return n
			}
		} else {
			prev = n
			stable = 0
			if time.Now().After(deadline) {
				return n
			}
		}
	}
}

// TestConsumeSink_DeliversInOrder verifies that a sink consumer receives its
// deliveries, in order, with the body intact — exercising the direct
// dispatch path (no buffering goroutine, no Go channel).
func TestConsumeSink_DeliversInOrder(t *testing.T) {
	const tag = "sink-ctag"

	rwc, srv := newSession(t)
	defer rwc.Close()

	go func() {
		srv.connectionOpen()
		srv.channelOpen(1)

		srv.recv(1, &basicConsume{})
		srv.send(1, &basicConsumeOk{ConsumerTag: tag})

		srv.send(1, &basicDeliver{ConsumerTag: tag, DeliveryTag: 1, Body: []byte("one")})
		srv.send(1, &basicDeliver{ConsumerTag: tag, DeliveryTag: 2, Body: []byte("two")})

		srv.recv(0, &connectionClose{})
		srv.send(0, &connectionCloseOk{})
		srv.C.Close()
	}()

	c, err := Open(rwc, defaultConfig())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ch, err := c.Channel()
	if err != nil {
		t.Fatalf("channel: %v", err)
	}

	got := make(chan Delivery, 4)
	gotTag, err := ch.ConsumeSink("queue", tag, true, false, false, false, nil, func(d Delivery) {
		got <- d
	})
	if err != nil {
		t.Fatalf("ConsumeSink: %v", err)
	}
	if gotTag != tag {
		t.Fatalf("returned tag = %q, want %q", gotTag, tag)
	}

	for i, want := range []struct {
		tag  uint64
		body string
	}{{1, "one"}, {2, "two"}} {
		select {
		case d := <-got:
			if d.DeliveryTag != want.tag {
				t.Fatalf("delivery %d: tag = %d, want %d", i, d.DeliveryTag, want.tag)
			}
			if string(d.Body) != want.body {
				t.Fatalf("delivery %d: body = %q, want %q", i, d.Body, want.body)
			}
			if d.ConsumerTag != tag {
				t.Fatalf("delivery %d: consumer tag = %q, want %q", i, d.ConsumerTag, tag)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for delivery %d", i)
		}
	}

	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// TestConsumeSink_NoPerConsumerGoroutines is the headline guarantee: many
// sink consumers cost O(1) goroutines, whereas the same number of Consume
// (channel) consumers cost O(N) (one buffering goroutine each).
func TestConsumeSink_NoPerConsumerGoroutines(t *testing.T) {
	const N = 200

	rwc, srv := newSession(t)
	defer rwc.Close()

	go func() {
		srv.connectionOpen()
		srv.channelOpen(1)
		// Answer 2N basic.consume requests (N sink + N channel).
		for i := 0; i < 2*N; i++ {
			m := srv.recv(1, &basicConsume{})
			bc := m.(*basicConsume)
			srv.send(1, &basicConsumeOk{ConsumerTag: bc.ConsumerTag})
		}
		srv.recv(0, &connectionClose{})
		srv.send(0, &connectionCloseOk{})
		srv.C.Close()
	}()

	c, err := Open(rwc, defaultConfig())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ch, err := c.Channel()
	if err != nil {
		t.Fatalf("channel: %v", err)
	}

	base := settleGoroutines()

	for i := 0; i < N; i++ {
		if _, err := ch.ConsumeSink("queue", "sink-"+strconv.Itoa(i), true, false, false, false, nil, func(Delivery) {}); err != nil {
			t.Fatalf("ConsumeSink #%d: %v", i, err)
		}
	}
	afterSink := settleGoroutines()

	for i := 0; i < N; i++ {
		if _, err := ch.Consume("queue", "chan-"+strconv.Itoa(i), true, false, false, false, nil); err != nil {
			t.Fatalf("Consume #%d: %v", i, err)
		}
	}
	afterChan := settleGoroutines()

	sinkGrowth := afterSink - base
	chanGrowth := afterChan - afterSink

	// Sinks must spawn essentially no goroutines; allow a tiny slack for
	// unrelated runtime churn.
	if sinkGrowth > N/10 {
		t.Fatalf("ConsumeSink spawned %d goroutines for %d consumers; want ~0", sinkGrowth, N)
	}
	// Channel consumers must spawn ~one buffering goroutine each.
	if chanGrowth < N*8/10 {
		t.Fatalf("Consume spawned only %d goroutines for %d consumers; want ~%d", chanGrowth, N, N)
	}

	t.Logf("goroutine growth: %d sink consumers => +%d, %d channel consumers => +%d", N, sinkGrowth, N, chanGrowth)

	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// TestConsumeSink_NilHandlerReturnsError verifies the nil-handler guard fires
// before any frame is written to the broker.
func TestConsumeSink_NilHandlerReturnsError(t *testing.T) {
	rwc, srv := newSession(t)
	defer rwc.Close()

	go func() {
		srv.connectionOpen()
		srv.channelOpen(1)
		// No basic.consume is expected: the guard must reject before any RPC.
		srv.recv(0, &connectionClose{})
		srv.send(0, &connectionCloseOk{})
		srv.C.Close()
	}()

	c, err := Open(rwc, defaultConfig())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ch, err := c.Channel()
	if err != nil {
		t.Fatalf("channel: %v", err)
	}

	tag, err := ch.ConsumeSink("queue", "ctag", true, false, false, false, nil, nil)
	if err == nil {
		t.Fatal("ConsumeSink with nil handler: want error, got nil")
	}
	if tag != "" {
		t.Fatalf("ConsumeSink with nil handler: tag = %q, want empty", tag)
	}

	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// TestConsumeSink_CancelStopsDelivery verifies that cancelling a sink consumer
// removes it: deliveries that arrive after cancel are dropped, not handled.
func TestConsumeSink_CancelStopsDelivery(t *testing.T) {
	const tag = "sink-cancel"

	rwc, srv := newSession(t)
	defer rwc.Close()

	go func() {
		srv.connectionOpen()
		srv.channelOpen(1)

		srv.recv(1, &basicConsume{})
		srv.send(1, &basicConsumeOk{ConsumerTag: tag})
		srv.send(1, &basicDeliver{ConsumerTag: tag, DeliveryTag: 1})

		srv.recv(1, &basicCancel{})
		srv.send(1, &basicCancelOk{ConsumerTag: tag})
		// This must be dropped: the sink is gone after cancel.
		srv.send(1, &basicDeliver{ConsumerTag: tag, DeliveryTag: 2})

		srv.recv(0, &connectionClose{})
		srv.send(0, &connectionCloseOk{})
		srv.C.Close()
	}()

	c, err := Open(rwc, defaultConfig())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ch, err := c.Channel()
	if err != nil {
		t.Fatalf("channel: %v", err)
	}

	var count atomic.Int64
	got := make(chan Delivery, 4)
	if _, err := ch.ConsumeSink("queue", tag, true, false, false, false, nil, func(d Delivery) {
		count.Add(1)
		got <- d
	}); err != nil {
		t.Fatalf("ConsumeSink: %v", err)
	}

	select {
	case d := <-got:
		if d.DeliveryTag != 1 {
			t.Fatalf("first delivery tag = %d, want 1", d.DeliveryTag)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first delivery")
	}

	if err := ch.Cancel(tag, false); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	// The post-cancel delivery (tag 2) must not reach the handler.
	select {
	case d := <-got:
		t.Fatalf("received delivery %d after cancel; want none", d.DeliveryTag)
	case <-time.After(250 * time.Millisecond):
	}

	if got := count.Load(); got != 1 {
		t.Fatalf("handler called %d times, want 1", got)
	}

	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}
