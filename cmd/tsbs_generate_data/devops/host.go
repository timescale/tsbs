package devops

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/common"
)

// Count of choices for auto-generated tag values:
const (
	machineRackChoicesPerDatacenter = 100
	machineServiceChoices           = 20
	machineServiceVersionChoices    = 2
	hostFmt                         = "host_%d"
)

type region struct {
	Name        []byte
	Datacenters [][]byte
}

var regions = []region{
	{
		[]byte("us-east-1"),
		[][]byte{
			[]byte("us-east-1a"),
			[]byte("us-east-1b"),
			[]byte("us-east-1c"),
			[]byte("us-east-1e"),
		},
	},
	{
		[]byte("us-west-1"),
		[][]byte{
			[]byte("us-west-1a"),
			[]byte("us-west-1b"),
		},
	},
	{
		[]byte("us-west-2"),
		[][]byte{
			[]byte("us-west-2a"),
			[]byte("us-west-2b"),
			[]byte("us-west-2c"),
		},
	},
	{
		[]byte("eu-west-1"),
		[][]byte{
			[]byte("eu-west-1a"),
			[]byte("eu-west-1b"),
			[]byte("eu-west-1c"),
		},
	},
	{
		[]byte("eu-central-1"),
		[][]byte{
			[]byte("eu-central-1a"),
			[]byte("eu-central-1b"),
		},
	},
	{
		[]byte("ap-southeast-1"),
		[][]byte{
			[]byte("ap-southeast-1a"),
			[]byte("ap-southeast-1b"),
		},
	},
	{
		[]byte("ap-southeast-2"),
		[][]byte{
			[]byte("ap-southeast-2a"),
			[]byte("ap-southeast-2b"),
		},
	},
	{
		[]byte("ap-northeast-1"),
		[][]byte{
			[]byte("ap-northeast-1a"),
			[]byte("ap-northeast-1c"),
		},
	},
	{
		[]byte("sa-east-1"),
		[][]byte{
			[]byte("sa-east-1a"),
			[]byte("sa-east-1b"),
			[]byte("sa-east-1c"),
		},
	},
}

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

	// MachineTagKeys fields common to all hosts:
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
)

// Host models a machine being monitored for dev ops
type Host struct {
	SimulatedMeasurements []common.SimulatedMeasurement

	// These are all assigned once, at Host creation:
	Name, Region, Datacenter, Rack, OS, Arch          []byte
	Team, Service, ServiceVersion, ServiceEnvironment []byte
}

func newHostMeasurements(start time.Time) []common.SimulatedMeasurement {
	return []common.SimulatedMeasurement{
		NewCPUMeasurement(start),
		NewDiskIOMeasurement(start),
		NewDiskMeasurement(start),
		NewKernelMeasurement(start),
		NewMemMeasurement(start),
		NewNetMeasurement(start),
		NewNginxMeasurement(start),
		NewPostgresqlMeasurement(start),
		NewRedisMeasurement(start),
	}
}

func newCPUOnlyHostMeasurements(start time.Time) []common.SimulatedMeasurement {
	return []common.SimulatedMeasurement{
		NewCPUMeasurement(start),
	}
}

func newCPUSingleHostMeasurements(start time.Time) []common.SimulatedMeasurement {
	return []common.SimulatedMeasurement{
		newSingleCPUMeasurement(start),
	}
}

// NewHost creates a new host in a simulated devops use case
func NewHost(i int, start time.Time) Host {
	return newHostWithMeasurementGenerator(i, start, newHostMeasurements)
}

// NewHostCPUOnly creates a new host in a simulated cpu-only use case, which is a subset of a devops case
// with only CPU metrics simulated
func NewHostCPUOnly(i int, start time.Time) Host {
	return newHostWithMeasurementGenerator(i, start, newCPUOnlyHostMeasurements)
}

// NewHostCPUSingle creates a new host in a simulated cpu-single use case, which is a subset of a devops case
// with only a single CPU metric is simulated
func NewHostCPUSingle(i int, start time.Time) Host {
	return newHostWithMeasurementGenerator(i, start, newCPUSingleHostMeasurements)
}

func newHostWithMeasurementGenerator(i int, start time.Time, generator func(time.Time) []common.SimulatedMeasurement) Host {
	sm := generator(start)

	region := randomRegionSliceChoice(regions)

	h := Host{
		// Tag Values that are static throughout the life of a Host:
		Name:               []byte(fmt.Sprintf(hostFmt, i)),
		Region:             region.Name,
		Datacenter:         common.RandomByteStringSliceChoice(region.Datacenters),
		Rack:               getByteStringRandomInt(machineRackChoicesPerDatacenter),
		Arch:               common.RandomByteStringSliceChoice(MachineArchChoices),
		OS:                 common.RandomByteStringSliceChoice(MachineOSChoices),
		Service:            getByteStringRandomInt(machineServiceChoices),
		ServiceVersion:     getByteStringRandomInt(machineServiceVersionChoices),
		ServiceEnvironment: common.RandomByteStringSliceChoice(MachineServiceEnvironmentChoices),
		Team:               common.RandomByteStringSliceChoice(MachineTeamChoices),

		SimulatedMeasurements: sm,
	}

	return h
}

// TickAll advances all Distributions of a Host.
func (h *Host) TickAll(d time.Duration) {
	for i := range h.SimulatedMeasurements {
		h.SimulatedMeasurements[i].Tick(d)
	}
}

func getByteStringRandomInt(limit int64) []byte {
	return []byte(fmt.Sprintf("%d", rand.Int63n(limit)))
}

func randomRegionSliceChoice(s []region) *region {
	return &s[rand.Intn(len(s))]
}
