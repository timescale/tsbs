package insertstrategy

import (
	"fmt"
	"math/rand"
	"time"
)

type nowProviderFn func() time.Time
type generateSleepTimeFn func() time.Duration

// SleepRegulator keeps the time required for each load worker
// to sleep between batch inserts. When calling the Sleep method
// for a worker, if required, the goroutine will sleep for the
// required amount of time
type SleepRegulator interface {
	// for a worker, if required, the goroutine will sleep for the
	// required amount of time
	Sleep(workerNum int, startedWorkAt time.Time)
}

type noWait struct{}

// NoWait returns a sleep regulator that doesn't make any worker sleep, at all.
func NoWait() SleepRegulator {
	return &noWait{}
}

func (n *noWait) Sleep(workerNum int, startedWorkAt time.Time) {
}

type sleepRegulator struct {
	sleepTimes map[int]generateSleepTimeFn
	nowFn      nowProviderFn
}

// NewSleepRegulator returns an implementation of the SleepRegulator interface,
// the insertIntervalString is parsed for a given number of workers (numWorkers). InsertIntervals
// are defined as minimum time between the start of two consecutive inserts. Time may be expressed
// as constant or range. It goes like this:
// numWorkers=2, string='0,1' => worker '0' insert ASAP, worker '1' at least 1 second between inserts
// numWorkers=2, string='2'=> worker '0' and all workers after it have at least 2 seconds between inserts
// numWorkers=3, string='1,2' => worker '0' at least 1 second, workers '1' and '2' at least 2 seconds between inserts
// numWorkers=1, string='0-1' => worker '0' needs to have [0,1) seconds between inserts
// numWorkers=3, string='1,2-4'=> worker '0' have 1 second between inserts, workers '1' and '2' have [2,4) seconds between inserts
func NewSleepRegulator(insertIntervalString string, numWorkers int, initialRand *rand.Rand) (SleepRegulator, error) {
	if numWorkers <= 0 {
		return nil, fmt.Errorf("number of workers must be positive, can't be %d", numWorkers)
	}

	sleepTimes, err := parseInsertIntervalString(insertIntervalString, numWorkers, initialRand)
	if err != nil {
		return nil, err
	}

	return &sleepRegulator{
		sleepTimes: sleepTimes,
		nowFn:      time.Now,
	}, nil
}

func (s *sleepRegulator) Sleep(workerNum int, startedWorkAt time.Time) {
	sleepGenerator, ok := s.sleepTimes[workerNum]
	if !ok {
		panic(fmt.Sprintf("invalid worker number: %d", workerNum))
	}

	// the worker should sleep this many seconds between inserts
	timeToSleep := sleepGenerator()
	// if started work at x, should sleep until x+timeToSleep
	shouldSleepUntil := startedWorkAt.Add(timeToSleep)
	now := s.nowFn()
	// if inserting took more time than required to sleep between inserts
	if !shouldSleepUntil.After(now) {
		return
	}

	durationToSleep := shouldSleepUntil.Sub(now)
	time.Sleep(durationToSleep)
}
