package devops

import (
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
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
	Name        string
	Datacenters []string
}

var regions = []region{
	{
		"us-east-1",
		[]string{
			"us-east-1a",
			"us-east-1b",
			"us-east-1c",
			"us-east-1e",
		},
	},
	{
		"us-west-1",
		[]string{
			"us-west-1a",
			"us-west-1b",
		},
	},
	{
		"us-west-2",
		[]string{
			"us-west-2a",
			"us-west-2b",
			"us-west-2c",
		},
	},
	{
		"eu-west-1",
		[]string{
			"eu-west-1a",
			"eu-west-1b",
			"eu-west-1c",
		},
	},
	{
		"eu-central-1",
		[]string{
			"eu-central-1a",
			"eu-central-1b",
		},
	},
	{
		"ap-southeast-1",
		[]string{
			"ap-southeast-1a",
			"ap-southeast-1b",
		},
	},
	{
		"ap-southeast-2",
		[]string{
			"ap-southeast-2a",
			"ap-southeast-2b",
		},
	},
	{
		"ap-northeast-1",
		[]string{
			"ap-northeast-1a",
			"ap-northeast-1c",
		},
	},
	{
		"sa-east-1",
		[]string{
			"sa-east-1a",
			"sa-east-1b",
			"sa-east-1c",
		},
	},
}

var (
	MachineTeamChoices = []string{
		"SF",
		"NYC",
		"LON",
		"CHI",
	}
	MachineOSChoices = []string{
		"Ubuntu16.10",
		"Ubuntu16.04LTS",
		"Ubuntu15.10",
	}
	MachineArchChoices = []string{
		"x64",
		"x86",
	}
	MachineServiceEnvironmentChoices = []string{
		"production",
		"staging",
		"test",
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

	// MachineTagType is a the type of all the tags, a dummy value string is
	MachineTagType = reflect.TypeOf("some string")
)

// Host models a machine being monitored for dev ops
type Host struct {
	SimulatedMeasurements []common.SimulatedMeasurement

	// These are all assigned once, at Host creation:
	Name               string
	Region             string
	Datacenter         string
	Rack               string
	OS                 string
	Arch               string
	Team               string
	Service            string
	ServiceVersion     string
	ServiceEnvironment string
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
		Name:               fmt.Sprintf(hostFmt, i),
		Region:             region.Name,
		Datacenter:         common.RandomStringSliceChoice(region.Datacenters),
		Rack:               getStringRandomInt(machineRackChoicesPerDatacenter),
		Arch:               common.RandomStringSliceChoice(MachineArchChoices),
		OS:                 common.RandomStringSliceChoice(MachineOSChoices),
		Service:            getStringRandomInt(machineServiceChoices),
		ServiceVersion:     getStringRandomInt(machineServiceVersionChoices),
		ServiceEnvironment: common.RandomStringSliceChoice(MachineServiceEnvironmentChoices),
		Team:               common.RandomStringSliceChoice(MachineTeamChoices),

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

func getStringRandomInt(limit int64) string {
	return strconv.FormatInt(rand.Int63n(limit), 10)
}

func randomRegionSliceChoice(s []region) *region {
	return &s[rand.Intn(len(s))]
}
