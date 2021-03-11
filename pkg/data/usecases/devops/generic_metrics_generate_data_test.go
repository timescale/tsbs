package devops

import (
	"fmt"
	"github.com/timescale/tsbs/pkg/data"
	"math"
	"testing"
	"time"
)

const dateLayout = "2006-01-02"

var startTime = getTime("2020-01-01")
var endTime time.Time = getTime("2020-01-02")

func getTime(strVal string) time.Time {
	time, err := time.Parse(dateLayout, strVal)
	if err != nil {
		panic(err)
	}
	return time
}

func TestGenericMetricsSimulatorFields(t *testing.T) {
	resetGenericMetricFields()
	metricCount := uint64(8)
	hostCount := uint64(10)
	config := getSimulatorConfig(hostCount, metricCount)
	simulator := config.NewSimulator(time.Hour, 0)
	fields := simulator.Fields()
	assertEqualInt(1, len(fields), "Wrong number of measurements", t)

	metricFields := fields[string(labelGenericMetrics)]
	if uint64(len(metricFields)) > metricCount {
		t.Errorf("Number of metric fields has to be less than %d", metricCount)
	}
}

func TestGenericMetricsSimulatorHosts(t *testing.T) {
	resetGenericMetricFields()
	metricCount := uint64(8)
	hostCount := uint64(10)
	config := getSimulatorConfig(hostCount, metricCount)
	simulator := config.NewSimulator(time.Hour, 0).(*GenericMetricsSimulator)
	assertEqualInt(len(simulator.hosts), int(hostCount), "Wrong number of hosts generated", t)

	for i, host := range simulator.hosts {
		if host.GenericMetricCount < 1 {
			t.Error("Host must have at least one metric")
		}
		if host.GenericMetricCount > metricCount {
			t.Errorf("Host can have %d metrics max", metricCount)
		}
		if host.StartEpoch != math.MaxUint64 {
			t.Error("Wrong host start epoch state")
		}
		if i < int(hostCount/2) {
			// first half of hosts should live forever
			assertEqualInt(0, int(host.EpochsToLive), "A half of hosts should live forever", t)
		} else if host.EpochsToLive < 1 || (host.EpochsToLive > simulator.epochs) {
			t.Error("A 2nd half of hosts should have limited life")
		}
	}
}

// we need to reset global variable between different tests
func resetGenericMetricFields() {
	genericMetricFields = nil
}

func TestGenericMetricsSimulatorRun(t *testing.T) {
	resetGenericMetricFields()
	hostCount := uint64(10)
	metricCount := uint64(10)
	config := getSimulatorConfig(hostCount, metricCount)
	simulator := config.NewSimulator(2*time.Hour, 0).(*GenericMetricsSimulator)

	pointsWrittenCnt := 0
	pointsNotWrittenCnt := 0
	hostPoints := make(map[string][]*data.Point, hostCount)

	for !simulator.Finished() {
		point := data.NewPoint()
		write := simulator.Next(point)
		if write {
			pointsWrittenCnt++
			hostname := point.GetTagValue(MachineTagKeys[0]).(string)
			if _, ok := hostPoints[hostname]; !ok {
				hostPoints[hostname] = make([]*data.Point, 0)
			}
			hostPoints[hostname] = append(hostPoints[hostname], point)

		} else {
			pointsNotWrittenCnt++
		}
	}

	// first half are forever living hosts containing point for each epoch
	for i := 0; i < len(hostPoints)/2; i++ {
		assertEqualInt(int(simulator.epochs), len(hostPoints[fmt.Sprintf("host_%d", i)]), fmt.Sprintf("Wrong host point distribution for host_%d", i), t)
	}

	// hosts with limited lifetime have less points
	assertHostPointDistribution(7, "host_5", hostPoints, t)
	assertHostPointDistribution(5, "host_6", hostPoints, t)
	assertHostPointDistribution(2, "host_7", hostPoints, t)
	assertHostPointDistribution(2, "host_8", hostPoints, t)
	assertHostPointDistribution(1, "host_9", hostPoints, t)

	// check metric distribution among hosts
	assertMetricCountDistribution(6, "host_0", hostPoints, t)
	assertMetricCountDistribution(1, "host_1", hostPoints, t)
	assertMetricCountDistribution(2, "host_2", hostPoints, t)
	assertMetricCountDistribution(1, "host_3", hostPoints, t)
	assertMetricCountDistribution(2, "host_4", hostPoints, t)
	assertMetricCountDistribution(5, "host_5", hostPoints, t)
	assertMetricCountDistribution(1, "host_6", hostPoints, t)
	assertMetricCountDistribution(2, "host_7", hostPoints, t)
	assertMetricCountDistribution(10, "host_8", hostPoints, t)
	assertMetricCountDistribution(10, "host_9", hostPoints, t)

	if pointsNotWrittenCnt > pointsWrittenCnt || uint64(pointsWrittenCnt) < simulator.maxPoints/2 {
		t.Errorf("Not enough points generated. Writen: %d, Not written: %d", pointsWrittenCnt, pointsNotWrittenCnt)
	}
}

func assertHostPointDistribution(count int, host string, hostPoints map[string][]*data.Point, t *testing.T) {
	assertEqualInt(count, len(hostPoints[host]), fmt.Sprintf("Wrong host point distribution for host %s", host), t)
}

func assertMetricCountDistribution(count int, host string, hostPoints map[string][]*data.Point, t *testing.T) {
	assertEqualInt(count, len(hostPoints[host][0].FieldKeys()), fmt.Sprintf("Wrong metric count distribution for host %s", host), t)
}

func getSimulatorConfig(hostCount, maxMetricCount uint64) GenericMetricsSimulatorConfig {
	return GenericMetricsSimulatorConfig{
		DevopsSimulatorConfig: &DevopsSimulatorConfig{
			Start: startTime,
			End:   endTime,

			InitHostCount:   uint64(math.Max(float64(1), float64(hostCount/2))),
			HostCount:       hostCount,
			HostConstructor: NewHostGenericMetrics,
			MaxMetricCount:  maxMetricCount,
		},
	}
}

func assertEqualInt(expected, got int, errMsg string, t *testing.T) {
	if expected != got {
		t.Errorf(errMsg+", Expected: %v, got: %v", expected, got)
	}
}
