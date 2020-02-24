package devops

import (
	"github.com/timescale/tsbs/pkg/data"
	"testing"
	"time"
)

const testDevopsHostCount = 100

var testDevopsConf = &DevopsSimulatorConfig{
	Start:           testTime,
	End:             testTime.Add(3 * time.Second),
	InitHostCount:   10,
	HostCount:       testDevopsHostCount,
	HostConstructor: NewHost,
}

func TestDevopsSimulatorNext(t *testing.T) {
	s := testDevopsConf.NewSimulator(time.Second, 0).(*DevopsSimulator)
	// There are two epochs for the test configuration, and a difference of 90
	// from init to final, so each epoch should add 45 devices to be written.
	writtenIdx := []int{10, 55, 100}
	p := data.NewPoint()
	totalPerRun := testDevopsHostCount * 9 // 9 subsystems per device

	runFn := func(run int) {
		for i := 0; i < totalPerRun; i++ {
			write := s.Next(p)
			hostIdx := i % testDevopsHostCount
			if got := int(s.hostIndex); got != hostIdx+1 {
				t.Errorf("run %d: host index incorrect, i = %d: got %d want %d", run, i, got, i+1)
			}
			if hostIdx < writtenIdx[run-1] && !write {
				t.Errorf("run %d: should write point at i = %d, but not", run, i)
			} else if hostIdx >= writtenIdx[run-1] && write {
				t.Errorf("run %d: should not write point at i = %d, but am", run, i)
			}

			if got := int(s.epoch); got != run-1 {
				t.Errorf("run %d: epoch prematurely turned over", run)
			}
		}
	}

	// First run through:
	runFn(1)
	// Second run through, should wrap around and do hosts again
	runFn(2)
	// Final run through, should be all hosts:
	runFn(3)
}

func TestDevopsSimulatorConfigNewSimulator(t *testing.T) {
	duration := time.Second
	start := time.Now()
	end := start.Add(10 * time.Second)
	numHosts := uint64(100)
	initHosts := uint64(0)
	conf := &DevopsSimulatorConfig{
		Start:           start,
		End:             end,
		InitHostCount:   initHosts,
		HostCount:       numHosts,
		HostConstructor: NewHost,
	}
	sim := conf.NewSimulator(duration, 0).(*DevopsSimulator)
	if got := sim.madePoints; got != 0 {
		t.Errorf("incorrect initial points: got %d want %d", got, 0)
	}
	if got := sim.epoch; got != 0 {
		t.Errorf("incorrect initial epoch: got %d want %d", got, 0)
	}
	if got := sim.hostIndex; got != 0 {
		t.Errorf("incorrect initial host index: got %d want %d", got, 0)
	}
	if got := sim.simulatedMeasurementIndex; got != 0 {
		t.Errorf("incorrect simulated measurement index: got %d want %d", got, 0)
	}
	if got := sim.epochHosts; got != initHosts {
		t.Errorf("incorrect initial epoch hosts: got %d want %d", got, initHosts)
	}
	if got := sim.initHosts; got != initHosts {
		t.Errorf("incorrect initial init hosts: got %d want %d", got, initHosts)
	}
	if got := sim.timestampStart; got != start {
		t.Errorf("incorrect start time: got %v want %v", got, start)
	}
	if got := sim.timestampEnd; got != end {
		t.Errorf("incorrect end time: got %v want %v", got, end)
	}
	wantEpochs := uint64(10) // 10 seconds between start & end, interval is 1s, so 10 / 1 = 10
	if got := sim.epochs; got != wantEpochs {
		t.Errorf("incorrect epochs: got %d want %d", got, wantEpochs)
	}
	wantMaxPoints := wantEpochs * numHosts * 9 // 9 measurements per host
	if got := sim.maxPoints; got != wantMaxPoints {
		t.Errorf("incorrect max points: got %d want %d", got, wantMaxPoints)
	}

}
