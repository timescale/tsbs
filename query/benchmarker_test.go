package query

import (
	"sync"
	"testing"
)

type testProcessor struct {
	count int
	wNum  int
}

func (p *testProcessor) Init(workerNum int) {
	p.wNum = workerNum
	p.count = 0
}

func (p *testProcessor) ProcessQuery(_ Query, _ bool) ([]*Stat, error) {
	p.count++
	return nil, nil
}

func TestProcessorHandler(t *testing.T) {
	qLimit := 17
	p1Num := 0
	p2Num := 5

	p1 := &testProcessor{}
	p2 := &testProcessor{}
	b := NewBenchmarkRunner()
	b.ch = make(chan Query, 2)

	var wg sync.WaitGroup
	qPool := &testQueryPool
	wg.Add(2)
	go b.processorHandler(&wg, qPool, p1, 0)
	go b.processorHandler(&wg, qPool, p2, 5)
	for i := 0; i < qLimit; i++ {
		q := qPool.Get().(*testQuery)
		b.ch <- q
	}
	close(b.ch)
	wg.Wait()

	if p1.wNum != p1Num {
		t.Errorf("p1 Init() not called: want %d got %d", p1Num, p1.wNum)
	}
	if p2.wNum != p2Num {
		t.Errorf("p2 Init() not called: want %d got %d", p2Num, p2.wNum)
	}
	if p1.count+p2.count != qLimit {
		t.Errorf("total queries wrong: want %d got %d", qLimit, p1.count+p2.count)
	}
}
