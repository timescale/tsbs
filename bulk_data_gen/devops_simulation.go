package main

import (
	"time"
)

// Measurement modes:
const (
	enumerationModeCPU = iota
	enumerationModeMem
)

// Count of choices for auto-generated tag values:
const (
	MachineRackChoicesPerDatacenter = 100
	MachineServiceChoices           = 20
	MachineServiceVersionChoices    = 2
)

var (
	MachineTeamChoices = [][]byte{
		[]byte("SF"),
		[]byte("NYC"),
		[]byte("LON"),
		[]byte("CHI"),
	}
	MachineOSChoices = [][]byte{
		[]byte("Ubuntu16.10"),
		[]byte("Ubuntu16.04LTS"),
		[]byte("Ubuntu15.10"),
	}
	MachineArchChoices = [][]byte{
		[]byte("x64"),
		[]byte("x86"),
	}
	MachineServiceEnvironmentChoices = [][]byte{
		[]byte("production"),
		[]byte("staging"),
		[]byte("test"),
	}
)

var (
	CPUByteString      = []byte("cpu")       // heap optimization
	CPUTotalByteString = []byte("cpu-total") // heap optimization
	MemoryByteString   = []byte("mem")       // heap optimization
)

var (
	// The duration of a log epoch.
	EpochDuration = 10 * time.Second

	// Choices for modeling a host's memory capacity.
	MemoryMaxBytesChoices = []int64{8 << 30, 12 << 30, 16 << 30}

	// Tag fields common to all hosts:
	MachineTagKeys = [][]byte{
		[]byte("hostname"),
		[]byte("region"),
		[]byte("datacenter"),
		[]byte("rack"),
		[]byte("os"),
		[]byte("arch"),
		[]byte("team"),
		[]byte("service"),
		[]byte("service_version"),
		[]byte("service_environment"),
	}

	// Field keys for 'cpu' points.
	CPUFieldKeys = [][]byte{
		[]byte("usage_user"),
		[]byte("usage_system"),
		[]byte("usage_idle"),
		[]byte("usage_nice"),
		[]byte("usage_iowait"),
		[]byte("usage_irq"),
		[]byte("usage_softirq"),
		[]byte("usage_steal"),
		[]byte("usage_guest"),
		[]byte("usage_guest_nice"),
	}

	// Field keys for 'mem' points.
	MemoryFieldKeys = [][]byte{
		[]byte("total"),
		[]byte("available"),
		[]byte("used"),
		[]byte("free"),
		[]byte("cached"),
		[]byte("buffered"),
		[]byte("used_percent"),
		[]byte("available_percent"),
	}
)

type Region struct {
	Name        []byte
	Datacenters [][]byte
}

var (
	// Choices of regions and their datacenters.
	Regions = []Region{
		{
			[]byte("us-east-1"), [][]byte{
				[]byte("us-east-1a"),
				[]byte("us-east-1b"),
				[]byte("us-east-1c"),
				[]byte("us-east-1e"),
			},
		},
		{
			[]byte("us-west-1"), [][]byte{
				[]byte("us-west-1a"),
				[]byte("us-west-1b"),
			},
		},
		{
			[]byte("us-west-2"), [][]byte{
				[]byte("us-west-2a"),
				[]byte("us-west-2b"),
				[]byte("us-west-2c"),
			},
		},
		{
			[]byte("eu-west-1"), [][]byte{
				[]byte("eu-west-1a"),
				[]byte("eu-west-1b"),
				[]byte("eu-west-1c"),
			},
		},
		{
			[]byte("eu-central-1"), [][]byte{
				[]byte("eu-central-1a"),
				[]byte("eu-central-1b"),
			},
		},
		{
			[]byte("ap-southeast-1"), [][]byte{
				[]byte("ap-southeast-1a"),
				[]byte("ap-southeast-1b"),
			},
		},
		{
			[]byte("ap-southeast-2"), [][]byte{
				[]byte("ap-southeast-2a"),
				[]byte("ap-southeast-2b"),
			},
		},
		{
			[]byte("ap-northeast-1"), [][]byte{
				[]byte("ap-northeast-1a"),
				[]byte("ap-northeast-1c"),
			},
		},
		{
			[]byte("sa-east-1"), [][]byte{
				[]byte("sa-east-1a"),
				[]byte("sa-east-1b"),
				[]byte("sa-east-1c"),
			},
		},
	}
)

