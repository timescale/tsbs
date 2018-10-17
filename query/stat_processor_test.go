package query

import (
	"testing"
	"time"
)

func TestStatProcessorSendStats(t *testing.T) {
	s := GetStat()
	s.isWarm = true
	statPool.Put(s)
	s = GetStat()
	if s.isWarm {
		t.Errorf("initial stat came back warm unexpectedly")
	}
	s.value = 10.1
	sp := &statProcessor{}
	sp.c = make(chan *Stat, 2)
	sp.sendStats([]*Stat{s, s})
	r := <-sp.c
	if r.value != s.value {
		t.Errorf("sent a stat and got a different one back")
	}
	if r.isWarm {
		t.Errorf("received stat is warm unexpectedly")
	}

	// 2nd value too
	r = <-sp.c
	if r.value != s.value {
		t.Errorf("sent a stat and got a different one back (2)")
	}
	if r.isWarm {
		t.Errorf("received stat is warm unexpectedly (2)")
	}

	// should not send anything
	wantLen := len(sp.c)
	sp.sendStats(nil)
	time.Sleep(25 * time.Millisecond)
	if got := len(sp.c); got != wantLen {
		t.Errorf("empty stat array changed channel length: got %d want %d", got, wantLen)
	}
}

func TestStatProcessorSendStatsWarm(t *testing.T) {
	s := GetStat()
	if s.isWarm {
		t.Errorf("initial stat came back warm unexpectedly")
	}
	s.value = 10.1
	sp := &statProcessor{}
	sp.c = make(chan *Stat, 2)
	sp.sendStatsWarm([]*Stat{s, s})
	r := <-sp.c
	if r.value != s.value {
		t.Errorf("sent a stat and got a different one back")
	}
	if !r.isWarm {
		t.Errorf("received stat is NOT warm unexpectedly")
	}

	// 2nd value too
	r = <-sp.c
	if r.value != s.value {
		t.Errorf("sent a stat and got a different one back (2)")
	}
	if !r.isWarm {
		t.Errorf("received stat is NOT warm unexpectedly (2)")
	}

	// should not send anything
	wantLen := len(sp.c)
	sp.sendStatsWarm(nil)
	time.Sleep(25 * time.Millisecond)
	if got := len(sp.c); got != wantLen {
		t.Errorf("empty stat array changed channel length: got %d want %d", got, wantLen)
	}
}
