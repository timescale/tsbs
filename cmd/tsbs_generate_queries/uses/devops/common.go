package devops

import (
	"fmt"
	"math/rand"
	"reflect"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	internalutils "github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/query"
)

const (
	allHosts                = "all hosts"
	errNHostsCannotNegative = "nHosts cannot be negative"
	errNoMetrics            = "cannot get 0 metrics"
	errTooManyMetrics       = "too many metrics asked for"
	errMoreItemsThanScale   = "cannot get random permutation with more items than scale"

	// DoubleGroupByDuration is the how big the time range for DoubleGroupBy query is
	DoubleGroupByDuration = 12 * time.Hour
	// HighCPUDuration is the how big the time range for HighCPU query is
	HighCPUDuration = 12 * time.Hour
	// MaxAllDuration is the how big the time range for MaxAll query is
	MaxAllDuration = 8 * time.Hour

	// LabelSingleGroupby is the label prefix for queries of the single groupby variety
	LabelSingleGroupby = "single-groupby"
	// LabelDoubleGroupby is the label prefix for queries of the double groupby variety
	LabelDoubleGroupby = "double-groupby"
	// LabelLastpoint is the label for the lastpoint query
	LabelLastpoint = "lastpoint"
	// LabelMaxAll is the label prefix for queries of the max all variety
	LabelMaxAll = "cpu-max-all"
	// LabelGroupbyOrderbyLimit is the label for groupby-orderby-limit query
	LabelGroupbyOrderbyLimit = "groupby-orderby-limit"
	// LabelHighCPU is the prefix for queries of the high-CPU variety
	LabelHighCPU = "high-cpu"
)

// Core is the common component of all generators for all systems
type Core struct {
	// Interval is the entire time range of the dataset
	Interval *internalutils.TimeInterval

	// Scale is the cardinality of the dataset in terms of devices/hosts
	Scale int
}

// NewCore returns a new Core for the given time range and cardinality
func NewCore(start, end time.Time, scale int) (*Core, error) {
	ti, err := internalutils.NewTimeInterval(start, end)
	if err != nil {
		return nil, err
	}

	return &Core{Interval: ti, Scale: scale}, nil
}

// GetRandomHosts returns a random set of nHosts from a given Core
func (d *Core) GetRandomHosts(nHosts int) ([]string, error) {
	return getRandomHosts(nHosts, d.Scale)
}

// cpuMetrics is the list of metric names for CPU
var cpuMetrics = []string{
	"usage_user",
	"usage_system",
	"usage_idle",
	"usage_nice",
	"usage_iowait",
	"usage_irq",
	"usage_softirq",
	"usage_steal",
	"usage_guest",
	"usage_guest_nice",
}

// GetCPUMetricsSlice returns a subset of metrics for the CPU
func GetCPUMetricsSlice(numMetrics int) ([]string, error) {
	if numMetrics <= 0 {
		return nil, fmt.Errorf(errNoMetrics)
	}
	if numMetrics > len(cpuMetrics) {
		return nil, fmt.Errorf(errTooManyMetrics)
	}
	return cpuMetrics[:numMetrics], nil
}

// GetAllCPUMetrics returns all the metrics for CPU
func GetAllCPUMetrics() []string {
	return cpuMetrics
}

// GetCPUMetricsLen returns the number of metrics in CPU
func GetCPUMetricsLen() int {
	return len(cpuMetrics)
}

// SingleGroupbyFiller is a type that can fill in a single groupby query
type SingleGroupbyFiller interface {
	GroupByTime(query.Query, int, int, time.Duration)
}

// DoubleGroupbyFiller is a type that can fill in a double groupby query
type DoubleGroupbyFiller interface {
	GroupByTimeAndPrimaryTag(query.Query, int)
}

// LastPointFiller is a type that can fill in a last point query
type LastPointFiller interface {
	LastPointPerHost(query.Query)
}

// MaxAllFiller is a type that can fill in a max all CPU metrics query
type MaxAllFiller interface {
	MaxAllCPU(query.Query, int)
}

// GroupbyOrderbyLimitFiller is a type that can fill in a groupby-orderby-limit query
type GroupbyOrderbyLimitFiller interface {
	GroupByOrderByLimit(query.Query)
}

// HighCPUFiller is a type that can fill in a high-cpu query
type HighCPUFiller interface {
	HighCPUForHosts(query.Query, int)
}

// GetDoubleGroupByLabel returns the Query human-readable label for DoubleGroupBy queries
func GetDoubleGroupByLabel(dbName string, numMetrics int) string {
	return fmt.Sprintf("%s mean of %d metrics, all hosts, random %s by 1h", dbName, numMetrics, DoubleGroupByDuration)
}

// GetHighCPULabel returns the Query human-readable label for HighCPU queries
func GetHighCPULabel(dbName string, nHosts int) (string, error) {
	label := dbName + " CPU over threshold, "
	if nHosts > 0 {
		label += fmt.Sprintf("%d host(s)", nHosts)
	} else if nHosts == 0 {
		label += allHosts
	} else {
		return "", fmt.Errorf("nHosts cannot be negative")
	}
	return label, nil
}

// GetMaxAllLabel returns the Query human-readable label for MaxAllCPU queries
func GetMaxAllLabel(dbName string, nHosts int) string {
	return fmt.Sprintf("%s max of all CPU metrics, random %4d hosts, random %s by 1h", dbName, nHosts, MaxAllDuration)
}

// getRandomHosts returns a subset of numHosts hostnames of a permutation of hostnames,
// numbered from 0 to totalHosts.
// Ex.: host_12, host_7, host_25 for numHosts=3 and totalHosts=30 (3 out of 30)
func getRandomHosts(numHosts int, totalHosts int) ([]string, error) {
	if numHosts < 1 {
		return nil, fmt.Errorf("number of hosts cannot be < 1; got %d", numHosts)
	}
	if numHosts > totalHosts {
		return nil, fmt.Errorf("number of hosts (%d) larger than total hosts. See --scale (%d)", numHosts, totalHosts)
	}

	randomNumbers, err := getRandomSubsetPerm(numHosts, totalHosts)
	if err != nil {
		return nil, err
	}

	hostnames := []string{}
	for _, n := range randomNumbers {
		hostnames = append(hostnames, fmt.Sprintf("host_%d", n))
	}

	return hostnames, nil
}

// getRandomSubsetPerm returns a subset of numItems of a permutation of numbers from 0 to totalNumbers,
// e.g., 5 items out of 30. This is an alternative to rand.Perm and then taking a sub-slice,
// which used up a lot more memory and slowed down query generation significantly.
// The subset of the permutation should have no duplicates and thus, can not be longer that original set
// Ex.: 12, 7, 25 for numItems=3 and totalItems=30 (3 out of 30)
func getRandomSubsetPerm(numItems int, totalItems int) ([]int, error) {
	if numItems > totalItems {
		// Cannot make a subset longer than the original set
		return nil, fmt.Errorf(errMoreItemsThanScale)
	}

	seen := map[int]bool{}
	res := []int{}
	for i := 0; i < numItems; i++ {
		for {
			n := rand.Intn(totalItems)
			// Keep iterating until a previously unseen int is found
			if !seen[n] {
				seen[n] = true
				res = append(res, n)
				break
			}
		}
	}
	return res, nil
}

func panicUnimplementedQuery(dg utils.DevopsGenerator) {
	panic(fmt.Sprintf("database (%v) does not implement query", reflect.TypeOf(dg)))
}
