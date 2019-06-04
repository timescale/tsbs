package insertstrategy

import (
	"math/rand"
	"testing"
	"time"
)

func TestNewSleepRegulator(t *testing.T) {
	testCases := []struct {
		desc           string
		intervalString string
		workers        int
		rand           *rand.Rand
		expectErr      bool
	}{
		{
			desc:      "Error on 0 workers",
			expectErr: true,
		}, {
			desc:           "Error on wrong interval string",
			expectErr:      true,
			intervalString: "a",
			workers:        1,
			rand:           rand.New(rand.NewSource(1)),
		}, {
			desc:           "Create 2 generators for 2 workers",
			expectErr:      false,
			intervalString: "1",
			workers:        1,
			rand:           rand.New(rand.NewSource(1)),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			res, err := NewSleepRegulator(tc.intervalString, tc.workers, tc.rand)
			if err != nil && !tc.expectErr {
				t.Errorf("unexpected error: %v", err)
				return
			} else if err == nil && tc.expectErr {
				t.Error("unexpected lack of error")
				return
			} else if tc.expectErr {
				return
			} else if res == nil {
				t.Error("sleep regulator not created")
				return
			}

			sr := res.(*sleepRegulator)
			if sr.nowFn == nil {
				t.Error("Current time provider function not set up")
			}

			for expectedWorker := 0; expectedWorker < tc.workers; expectedWorker++ {
				if sr.sleepTimes[expectedWorker] == nil {
					t.Errorf("sleep generator fn for worker %d is missing", expectedWorker)
					break
				}
			}
		})

	}
}

func TestTimeToSleepPanicOnWrongWorkerNumber(t *testing.T) {
	sr, _ := NewSleepRegulator("1", 1, rand.New(rand.NewSource(0)))
	defer func() {
		if r := recover(); r != "invalid worker number: 2" {
			t.Errorf("wrong panic.\nexpected: invalid worker number: 1\ngot: %v", r)
		}
	}()
	sr.Sleep(2, time.Now())
	t.Errorf("the code did not panic")
}

func TestTimeToSleep(t *testing.T) {
	testWorkStart, _ := time.Parse(time.RFC3339, "2019-01-01T00:00:00Z")
	testCases := []struct {
		desc              string
		sleepTime         time.Duration
		expectedSleepTime time.Duration
		currentTime       time.Time
	}{
		{
			desc:              "sleep for max sleep time",
			sleepTime:         time.Microsecond,
			expectedSleepTime: time.Microsecond,
			currentTime:       testWorkStart,
		}, {
			desc:              "sleep for half of the max sleep time",
			sleepTime:         2 * time.Millisecond,
			expectedSleepTime: time.Millisecond,
			currentTime:       testWorkStart.Add(time.Millisecond),
		}, {
			desc:              "sleep for part of max sleep time",
			sleepTime:         time.Second,
			expectedSleepTime: 496 * time.Millisecond,
			currentTime:       testWorkStart.Add((1000 - 496) * time.Millisecond),
		}, {
			desc:              "don't sleep, work was longer than expected sleep time",
			sleepTime:         time.Nanosecond,
			expectedSleepTime: 0 * time.Nanosecond,
			currentTime:       testWorkStart.Add(2 * time.Nanosecond),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			sr := &sleepRegulator{
				sleepTimes: map[int]generateSleepTimeFn{
					0: func() time.Duration { return tc.sleepTime },
				},
				nowFn: func() time.Time { return tc.currentTime },
			}
			start := time.Now()
			sr.Sleep(0, testWorkStart)
			end := time.Now()
			shouldSleepUntil := start.Add(tc.expectedSleepTime)
			if shouldSleepUntil.After(end) {
				t.Errorf("expected to sleep until %v, woke up at %v", shouldSleepUntil, end)
			}
		})
	}
}
