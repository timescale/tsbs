package insertstrategy

import (
	"errors"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

const (
	workerSleepUnit     = time.Second
	intervalSeparator   = ","
	rangeSeparator      = "-"
	intervalFormatError = "worker insert interval could not be parsed as integer constant or range. Required: 'x' or 'x-y' | x,y are uint x<y"
)

// parseInsertIntervalString parses a string representation of insert intervals for a given
// number of workers (numWorkers). InsertIntervals are defined as minimum time between
// the start of two consecutive inserts. It goes like this:
// numWorkers=2, string='0,1' => worker '0' insert ASAP, worker '1' at least 1 second between inserts
// numWorkers=2, string='2'=> worker '0' and all workers after it have at least 2 seconds between inserts
// numWorkers=3, string='1,2' => worker '0' at least 1 second, workers '1' and '2' at least 2 seconds between inserts
// numWorkers=1, string='0-1' => worker '0' needs to have [0,1) seconds between inserts
// numWorkers=3, string='1,2-4'=> worker '0' have 1 second between inserts, workers '1' and '2' have [2,4) seconds between inserts
// Error returned if numbers can't be parsed
func parseInsertIntervalString(insertIntervalString string, numWorkers int, initialRand *rand.Rand) (map[int]generateSleepTimeFn, error) {
	randsPerWorker := makeRandsForWorkers(numWorkers, initialRand)
	splitIntervals := splitIntervalString(insertIntervalString)
	numIntervals := len(splitIntervals)
	sleepGenerators := make(map[int]generateSleepTimeFn)
	currentInterval := 0
	var err error

	for i := 0; i < numWorkers; i++ {
		intervalToParse := splitIntervals[currentInterval]
		sleepGenerators[i], err = parseSingleIntervalString(intervalToParse, randsPerWorker[i])
		if err != nil {
			return nil, err
		}

		if currentInterval < numIntervals-1 {
			currentInterval++
		}
	}

	return sleepGenerators, nil
}

// parses an insert interval string for a single worker,
// first it attempts to parse it as a constant, then as a range
func parseSingleIntervalString(rangeStr string, randForWorker *rand.Rand) (generateSleepTimeFn, error) {
	if number, err := strconv.Atoi(rangeStr); err == nil {
		return newConstantSleepTimeGenerator(number), nil
	}

	if numbers, err := attemptRangeParse(rangeStr); err == nil {
		return newRangeSleepTimeGenerator(numbers[0], numbers[1], randForWorker), nil
	}

	return nil, errors.New(intervalFormatError)
}

// attempts to parse a ranged sleep interval ('2-5')
// errors returned if interval is not split by -,
// parts are not integers or first part is a larger integer
// than the second
func attemptRangeParse(rangeString string) ([]int, error) {
	parts := strings.SplitN(rangeString, rangeSeparator, 2)
	if len(parts) != 2 {
		return nil, errors.New(intervalFormatError)
	}

	var first, second int
	var err error
	if first, err = strconv.Atoi(parts[0]); err != nil {
		return nil, errors.New(intervalFormatError)
	}

	if second, err = strconv.Atoi(parts[1]); err != nil {
		return nil, errors.New(intervalFormatError)
	}

	if first >= second {
		return nil, errors.New(intervalFormatError)
	}

	return []int{first, second}, nil
}

// splits a sleep interval config string ('1,2-5,4') to individual
// const or range sleep times ('1', '2-5','4')
func splitIntervalString(insertIntervalString string) []string {
	if insertIntervalString == "" {
		return []string{"0"}
	}
	return strings.Split(insertIntervalString, intervalSeparator)
}

// an initialRand generator is used to give the seeds for new rand generators
// that will be used by the workers when asking how much should they sleep
// used only for irregular sleep patterns
func makeRandsForWorkers(num int, initialRand *rand.Rand) []*rand.Rand {
	toReturn := make([]*rand.Rand, num)
	for i := 0; i < num; i++ {
		seed := initialRand.Int63()
		src := rand.NewSource(seed)
		toReturn[i] = rand.New(src)
	}

	return toReturn
}

// returns a function that always generates the same number (maxSleepTime)
func newConstantSleepTimeGenerator(maxSleepTime int) generateSleepTimeFn {
	maxSleepDuration := time.Duration(maxSleepTime) * workerSleepUnit
	return func() time.Duration {
		return maxSleepDuration
	}
}

// returns a function that can generate a random integer in the range [minSleepTime, maxSleepTime]
func newRangeSleepTimeGenerator(minSleepTime, maxSleepTime int, randToUse *rand.Rand) generateSleepTimeFn {
	if randToUse == nil {
		panic("random number generator passed to range sleep generator was nil")
	}
	return func() time.Duration {
		sleep := minSleepTime + randToUse.Intn(maxSleepTime-minSleepTime)
		return time.Duration(sleep) * workerSleepUnit
	}
}
