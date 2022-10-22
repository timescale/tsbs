package iotdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestModifyHostnames(t *testing.T) {
	cases := []struct {
		description string
		hostnames   []string
		expected    []string
	}{
		{
			description: "normal node names",
			hostnames:   []string{"hostname", "hello_world"},
			expected:    []string{"hostname", "hello_world"},
		},
		{
			description: "IP address or URL as hostnames",
			hostnames:   []string{"192.168.1.1", "8.8.8.8", "iotdb.apache.org"},
			expected:    []string{"\"192.168.1.1\"", "\"8.8.8.8\"", "\"iotdb.apache.org\""},
		},
		{
			description: "already modified case",
			hostnames:   []string{"\"192.168.1.1\"", "\"8.8.8.8\"", "\"iotdb.apache.org\""},
			expected:    []string{"\"192.168.1.1\"", "\"8.8.8.8\"", "\"iotdb.apache.org\""},
		},
		{
			description: "mixed host names",
			hostnames:   []string{"192.168.1.1", "hostname", "iotdb.apache.org", "\"8.8.8.8\""},
			expected:    []string{"\"192.168.1.1\"", "hostname", "\"iotdb.apache.org\"", "\"8.8.8.8\""},
		},
	}

	for _, c := range cases {
		t.Run(c.description, func(t *testing.T) {
			b := BaseGenerator{BasicPath: "root", BasicPathLevel: 0}
			dq, err := b.NewDevops(time.Now(), time.Now(), 10)
			require.NoError(t, err, "Error while creating devops generator")
			d := dq.(*Devops)

			actual := d.modifyHostnames(c.hostnames)
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
			hostnames:      []string{"host1", "192.168.1.1", "\"iotdb.apache.org\""},
			expected:       "root.cpu.host1, root.cpu.\"192.168.1.1\", root.cpu.\"iotdb.apache.org\"",
		},
	}

	for _, c := range cases {
		t.Run(c.description, func(t *testing.T) {
			b := BaseGenerator{BasicPath: c.basicPath, BasicPathLevel: c.basicPathLevel}
			dq, err := b.NewDevops(time.Now(), time.Now(), 10)
			require.NoError(t, err, "Error while creating devops generator")
			d := dq.(*Devops)

			actual := d.getHostFromWithHostnames(c.hostnames)
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
			dq, err := b.NewDevops(time.Now(), time.Now(), 10)
			require.NoError(t, err, "Error while creating devops generator")
			d := dq.(*Devops)

			actual := d.getSelectClausesAggMetricsString(c.agg, c.metrics)
			require.EqualValues(t, c.expected, actual)
		})
	}
}
