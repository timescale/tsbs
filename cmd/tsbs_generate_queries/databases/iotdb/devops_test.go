package iotdb

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
)

func TestModifyHostnames(t *testing.T) {
	cases := []struct {
		description string
		hostnames   []string
		expected    []string
	}{
		{
			description: "normal node name",
			hostnames:   []string{"hostname", "hello_world"},
			expected:    []string{"hostname", "hello_world"},
		},
		{
			description: "IP address or URL as hostnames",
			hostnames:   []string{"192.168.1.1", "8.8.8.8", "iotdb.apache.org"},
			expected:    []string{"`192.168.1.1`", "`8.8.8.8`", "`iotdb.apache.org`"},
		},
		{
			description: "already modified cases",
			hostnames:   []string{"`192.168.1.1`", "`8.8.8.8`", "`iotdb.apache.org`"},
			expected:    []string{"`192.168.1.1`", "`8.8.8.8`", "`iotdb.apache.org`"},
		},
		{
			description: "mixed host names",
			hostnames:   []string{"192.168.1.1", "hostname", "iotdb.apache.org", "`8.8.8.8`"},
			expected:    []string{"`192.168.1.1`", "hostname", "`iotdb.apache.org`", "`8.8.8.8`"},
		},
	}

	for _, c := range cases {
		t.Run(c.description, func(t *testing.T) {
			b := BaseGenerator{BasicPath: "root", BasicPathLevel: 0}
			queryGenerator, err := b.NewDevops(time.Now(), time.Now(), 10)
			require.NoError(t, err, "Error while creating devops generator")
			dp := queryGenerator.(*Devops)

			actual := dp.modifyHostnames(c.hostnames)
			require.EqualValues(t, c.expected, actual)
		})
	}
}

func TestDevopsGetHostFromString(t *testing.T) {
	cases := []struct {
		description    string
		basicPath      string
		basicPathLevel int32
		hostnames      []string
		expected       string
	}{
		{
			description:    "single host",
			basicPath:      "root",
			basicPathLevel: 0,
			hostnames:      []string{"host1"},
			expected:       "root.cpu.host1",
		},
		{
			description:    "multi host (2)",
			basicPath:      "root",
			basicPathLevel: 0,
			hostnames:      []string{"host1", "host2"},
			expected:       "root.cpu.host1, root.cpu.host2",
		},
		{
			description:    "multi host (2) with storage group",
			basicPath:      "root.ln",
			basicPathLevel: 1,
			hostnames:      []string{"host1", "host2"},
			expected:       "root.ln.cpu.host1, root.ln.cpu.host2",
		},
		{
			description:    "multi host (3) with special node names",
			basicPath:      "root",
			basicPathLevel: 0,
			hostnames:      []string{"host1", "192.168.1.1", "`iotdb.apache.org`"},
			expected:       "root.cpu.host1, root.cpu.`192.168.1.1`, root.cpu.`iotdb.apache.org`",
		},
	}

	for _, c := range cases {
		t.Run(c.description, func(t *testing.T) {
			b := BaseGenerator{BasicPath: c.basicPath, BasicPathLevel: c.basicPathLevel}
			queryGenerator, err := b.NewDevops(time.Now(), time.Now(), 10)
			require.NoError(t, err, "Error while creating devops generator")
			dp := queryGenerator.(*Devops)

			actual := dp.getHostFromWithHostnames(c.hostnames)
			require.EqualValues(t, c.expected, actual)
		})
	}
}

func TestDevopsGetSelectClausesAggMetricsString(t *testing.T) {
	cases := []struct {
		description string
		agg         string
		metrics     []string
		expected    string
	}{
		{
			description: "single metric - max",
			agg:         "MAX_VALUE",
			metrics:     []string{"value"},
			expected:    "MAX_VALUE(value)",
		},
		{
			description: "multiple metric - max",
			agg:         "MAX_VALUE",
			metrics:     []string{"temperature", "frequency"},
			expected:    "MAX_VALUE(temperature), MAX_VALUE(frequency)",
		},
		{
			description: "multiple metric - avg",
			agg:         "AVG",
			metrics:     []string{"temperature", "frequency"},
			expected:    "AVG(temperature), AVG(frequency)",
		},
	}

	for _, c := range cases {
		t.Run(c.description, func(t *testing.T) {
			b := BaseGenerator{BasicPath: "root", BasicPathLevel: 0}
			queryGenerator, err := b.NewDevops(time.Now(), time.Now(), 10)
			require.NoError(t, err, "Error while creating devops generator")
			d := queryGenerator.(*Devops)

			actual := d.getSelectClausesAggMetricsString(c.agg, c.metrics)
			require.EqualValues(t, c.expected, actual)
		})
	}
}

func TestGroupByTime(t *testing.T) {
	rand.Seed(123) // Setting seed for testing purposes.
	start := time.Unix(0, 0)
	end := start.Add(time.Hour)
	base := BaseGenerator{BasicPath: "root", BasicPathLevel: 0}
	queryGenerator, err := base.NewDevops(start, end, 10)
	require.NoError(t, err, "Error while creating devops generator")
	dp := queryGenerator.(*Devops)

	metrics := 1
	nHosts := 1
	duration := time.Second

	actual := dp.GenerateEmptyQuery()
	expected := dp.GenerateEmptyQuery()
	dp.fillInQuery(expected,
		"IoTDB 1 cpu metric(s), random    1 hosts, random 1s by 1m",
		"IoTDB 1 cpu metric(s), random    1 hosts, random 1s by 1m: 1970-01-01T00:05:58Z",
		"SELECT MAX_VALUE(usage_user) FROM root.cpu.host_9 GROUP BY ([1970-01-01 00:05:58, 1970-01-01 00:05:59), 1m), LEVEL = 1",
	)
	dp.GroupByTime(actual, nHosts, metrics, duration)

	require.EqualValues(t, expected, actual)
}

func TestGroupByTimeAndPrimaryTag(t *testing.T) {
	cases := []struct {
		description        string
		numMetrics         int
		baseGenerator      BaseGenerator
		expectedHumanLabel string
		expectedHumanDesc  string
		expectedSQLQuery   string
	}{
		{
			description:        "1 metric with storage group 'root.sg'",
			numMetrics:         1,
			baseGenerator:      BaseGenerator{BasicPath: "root.sg", BasicPathLevel: 1},
			expectedHumanLabel: "IoTDB mean of 1 metrics, all hosts, random 12h0m0s by 1h",
			expectedHumanDesc:  "IoTDB mean of 1 metrics, all hosts, random 12h0m0s by 1h: 1970-01-01T00:16:22Z",
			expectedSQLQuery:   "SELECT AVG(usage_user) FROM root.sg.cpu.* GROUP BY ([1970-01-01 00:16:22, 1970-01-01 12:16:22), 1h)",
		},
		{
			description:        "5 metric with storage group 'root'",
			numMetrics:         5,
			baseGenerator:      BaseGenerator{BasicPath: "root", BasicPathLevel: 0},
			expectedHumanLabel: "IoTDB mean of 5 metrics, all hosts, random 12h0m0s by 1h",
			expectedHumanDesc:  "IoTDB mean of 5 metrics, all hosts, random 12h0m0s by 1h: 1970-01-01T00:16:22Z",
			expectedSQLQuery:   "SELECT AVG(usage_user), AVG(usage_system), AVG(usage_idle), AVG(usage_nice), AVG(usage_iowait) FROM root.cpu.* GROUP BY ([1970-01-01 00:16:22, 1970-01-01 12:16:22), 1h)",
		},
	}

	start := time.Unix(0, 0)
	end := start.Add(devops.DoubleGroupByDuration).Add(time.Hour)

	for _, c := range cases {
		t.Run(c.description, func(t *testing.T) {
			rand.Seed(123) // Setting seed for testing purposes.
			b := c.baseGenerator
			queryGenerator, err := b.NewDevops(start, end, 10)
			require.NoError(t, err, "Error while creating devops generator")
			dp := queryGenerator.(*Devops)

			actual := dp.GenerateEmptyQuery()
			expected := dp.GenerateEmptyQuery()

			dp.fillInQuery(expected, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedSQLQuery)
			dp.GroupByTimeAndPrimaryTag(actual, c.numMetrics)

			require.EqualValues(t, expected, actual)
		})
	}
}

func TestLastPointPerHost(t *testing.T) {
	rand.Seed(123) // Setting seed for testing purposes.
	base := BaseGenerator{BasicPath: "root.sg", BasicPathLevel: 1}
	queryGenerator, err := base.NewDevops(time.Now(), time.Now(), 10)
	require.NoError(t, err, "Error while creating devops generator")
	dp := queryGenerator.(*Devops)

	actual := dp.GenerateEmptyQuery()
	expected := dp.GenerateEmptyQuery()
	dp.fillInQuery(expected,
		"IoTDB last row per host",
		"IoTDB last row per host: cpu",
		"SELECT LAST * FROM root.sg.cpu.*",
	)
	dp.LastPointPerHost(actual)

	require.EqualValues(t, expected, actual)
}

func TestMaxAllCPU(t *testing.T) {
	cases := []struct {
		description        string
		nHosts             int
		baseGenerator      BaseGenerator
		expectedHumanLabel string
		expectedHumanDesc  string
		expectedSQLQuery   string
	}{
		{
			description:        "1 host with storage group 'root'",
			nHosts:             1,
			baseGenerator:      BaseGenerator{BasicPath: "root", BasicPathLevel: 0},
			expectedHumanLabel: "IoTDB max of all CPU metrics, random    1 hosts, random 8h0m0s by 1h",
			expectedHumanDesc:  "IoTDB max of all CPU metrics, random    1 hosts, random 8h0m0s by 1h: 1970-01-01T02:16:22Z",
			expectedSQLQuery: "SELECT MAX_VALUE(usage_user), MAX_VALUE(usage_system), MAX_VALUE(usage_idle), " +
				"MAX_VALUE(usage_nice), MAX_VALUE(usage_iowait), MAX_VALUE(usage_irq), MAX_VALUE(usage_softirq), " +
				"MAX_VALUE(usage_steal), MAX_VALUE(usage_guest), MAX_VALUE(usage_guest_nice) " +
				"FROM root.cpu.host_9 GROUP BY ([1970-01-01 02:16:22, 1970-01-01 10:16:22), 1h), LEVEL=1",
		},
		{
			description:        "3 hosts with storage group 'root'",
			nHosts:             3,
			baseGenerator:      BaseGenerator{BasicPath: "root", BasicPathLevel: 0},
			expectedHumanLabel: "IoTDB max of all CPU metrics, random    3 hosts, random 8h0m0s by 1h",
			expectedHumanDesc:  "IoTDB max of all CPU metrics, random    3 hosts, random 8h0m0s by 1h: 1970-01-01T02:16:22Z",
			expectedSQLQuery: "SELECT MAX_VALUE(usage_user), MAX_VALUE(usage_system), MAX_VALUE(usage_idle), " +
				"MAX_VALUE(usage_nice), MAX_VALUE(usage_iowait), MAX_VALUE(usage_irq), MAX_VALUE(usage_softirq), " +
				"MAX_VALUE(usage_steal), MAX_VALUE(usage_guest), MAX_VALUE(usage_guest_nice) " +
				"FROM root.cpu.host_9, root.cpu.host_3, root.cpu.host_5 GROUP BY ([1970-01-01 02:16:22, 1970-01-01 10:16:22), 1h), LEVEL=1",
		},
	}

	start := time.Unix(0, 0)
	end := start.Add(devops.DoubleGroupByDuration).Add(time.Hour)

	for _, c := range cases {
		t.Run(c.description, func(t *testing.T) {
			rand.Seed(123) // Setting seed for testing purposes.
			b := c.baseGenerator
			queryGenerator, err := b.NewDevops(start, end, 10)
			require.NoError(t, err, "Error while creating devops generator")
			dp := queryGenerator.(*Devops)

			actual := dp.GenerateEmptyQuery()
			expected := dp.GenerateEmptyQuery()

			dp.fillInQuery(expected, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedSQLQuery)
			dp.MaxAllCPU(actual, c.nHosts, devops.MaxAllDuration)

			require.EqualValues(t, expected, actual)
		})
	}
}

func TestGroupByOrderByLimit(t *testing.T) {
	rand.Seed(123) // Setting seed for testing purposes.
	base := BaseGenerator{BasicPath: "root", BasicPathLevel: 0}
	start := time.Unix(0, 0)
	end := start.Add(2 * time.Hour)
	queryGenerator, err := base.NewDevops(start, end, 10)
	require.NoError(t, err, "Error while creating devops generator")
	dp := queryGenerator.(*Devops)

	actual := dp.GenerateEmptyQuery()
	expected := dp.GenerateEmptyQuery()
	dp.fillInQuery(expected,
		"IoTDB max cpu over last 5 min-intervals (random end)",
		"IoTDB max cpu over last 5 min-intervals (random end): 1970-01-01T00:16:22Z",
		"SELECT MAX_VALUE(usage_user) FROM root.cpu.* GROUP BY ([1970-01-01 01:11:22, 1970-01-01 01:16:22), 1m), LEVEL = 1 ORDER BY TIME DESC LIMIT 5",
	)
	dp.GroupByOrderByLimit(actual)

	require.EqualValues(t, expected, actual)
}

func TestHighCPUForHosts(t *testing.T) {
	cases := []struct {
		description        string
		nHosts             int
		baseGenerator      BaseGenerator
		expectedHumanLabel string
		expectedHumanDesc  string
		expectedSQLQuery   string
	}{
		{
			description:        "ALL host with storage group 'root'",
			nHosts:             0,
			baseGenerator:      BaseGenerator{BasicPath: "root", BasicPathLevel: 0},
			expectedHumanLabel: "IoTDB CPU over threshold, all hosts",
			expectedHumanDesc:  "IoTDB CPU over threshold, all hosts: 1970-01-01T00:16:22Z",
			expectedSQLQuery:   "SELECT * FROM root.cpu.* WHERE usage_user > 90 AND time >= 1970-01-01 00:16:22 AND time < 1970-01-01 12:16:22 ALIGN BY DEVICE",
		},
		{
			description:        "1 host with storage group 'root.sg.abc'",
			nHosts:             1,
			baseGenerator:      BaseGenerator{BasicPath: "root.sg.abc", BasicPathLevel: 2},
			expectedHumanLabel: "IoTDB CPU over threshold, 1 host(s)",
			expectedHumanDesc:  "IoTDB CPU over threshold, 1 host(s): 1970-01-01T00:16:22Z",
			expectedSQLQuery:   "SELECT * FROM root.sg.abc.cpu.host_9 WHERE usage_user > 90 AND time >= 1970-01-01 00:16:22 AND time < 1970-01-01 12:16:22 ALIGN BY DEVICE",
		},
		{
			description:        "5 host2 with storage group 'root.ln'",
			nHosts:             5,
			baseGenerator:      BaseGenerator{BasicPath: "root.ln", BasicPathLevel: 1},
			expectedHumanLabel: "IoTDB CPU over threshold, 5 host(s)",
			expectedHumanDesc:  "IoTDB CPU over threshold, 5 host(s): 1970-01-01T00:16:22Z",
			expectedSQLQuery:   "SELECT * FROM root.ln.cpu.host_9, root.ln.cpu.host_3, root.ln.cpu.host_5, root.ln.cpu.host_1, root.ln.cpu.host_7 WHERE usage_user > 90 AND time >= 1970-01-01 00:16:22 AND time < 1970-01-01 12:16:22 ALIGN BY DEVICE",
		},
	}

	start := time.Unix(0, 0)
	end := start.Add(devops.HighCPUDuration).Add(time.Hour)

	for _, c := range cases {
		t.Run(c.description, func(t *testing.T) {
			rand.Seed(123) // Setting seed for testing purposes.
			b := c.baseGenerator
			queryGenerator, err := b.NewDevops(start, end, 10)
			require.NoError(t, err, "Error while creating devops generator")
			dp := queryGenerator.(*Devops)

			actual := dp.GenerateEmptyQuery()
			expected := dp.GenerateEmptyQuery()

			dp.fillInQuery(expected, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedSQLQuery)
			dp.HighCPUForHosts(actual, c.nHosts)
			fmt.Println(actual)

			require.EqualValues(t, expected, actual)
		})
	}
}
