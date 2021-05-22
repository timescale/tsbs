package devops

import (
	"fmt"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"math"
	"math/rand"
	"reflect"
	"strconv"
	"time"
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

	// machineTagType is the type of all the tags (string)
	// to be used by TagTypes. Not used elsewhere.
	machineTagType = reflect.TypeOf("some string")
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

	// needed for generic use-casea
	GenericMetricCount uint64 // number of metrics generated
	StartEpoch         uint64
	EpochsToLive       uint64 // 0 means forever
}

type generator func(ctx *HostContext) []common.SimulatedMeasurement

func newHostMeasurements(ctx *HostContext) []common.SimulatedMeasurement {
	return []common.SimulatedMeasurement{
		NewCPUMeasurement(ctx.start),
		NewDiskIOMeasurement(ctx.start),
		NewDiskMeasurement(ctx.start),
		NewKernelMeasurement(ctx.start),
		NewMemMeasurement(ctx.start),
		NewNetMeasurement(ctx.start),
		NewNginxMeasurement(ctx.start),
		NewPostgresqlMeasurement(ctx.start),
		NewRedisMeasurement(ctx.start),
	}
}

func newCPUOnlyHostMeasurements(ctx *HostContext) []common.SimulatedMeasurement {
	return []common.SimulatedMeasurement{
		NewCPUMeasurement(ctx.start),
	}
}

func newCPUSingleHostMeasurements(ctx *HostContext) []common.SimulatedMeasurement {
	return []common.SimulatedMeasurement{
		newSingleCPUMeasurement(ctx.start),
	}
}

func newGenericHostMeasurements(ctx *HostContext) []common.SimulatedMeasurement {
	return []common.SimulatedMeasurement{NewGenericMeasurements(ctx.start, ctx.metricCount)}
}

// NewHost creates a new host in a simulated devops use case
func NewHost(ctx *HostContext) Host {
	return newHostWithMeasurementGenerator(newHostMeasurements, ctx)
}

// NewHostCPUOnly creates a new host in a simulated cpu-only use case, which is a subset of a devops case
// with only CPU metrics simulated
func NewHostCPUOnly(ctx *HostContext) Host {
	return newHostWithMeasurementGenerator(newCPUOnlyHostMeasurements, ctx)
}

// NewHostCPUSingle creates a new host in a simulated cpu-single use case, which is a subset of a devops case
// with only a single CPU metric is simulated
func NewHostCPUSingle(ctx *HostContext) Host {
	return newHostWithMeasurementGenerator(newCPUSingleHostMeasurements, ctx)
}

// NewHostGenericMetrics creates a new host in simulated generic metrics use case. Useful for testing with
// high cardinality metrics
func NewHostGenericMetrics(ctx *HostContext) Host {
	return newHostWithMeasurementGenerator(newGenericHostMeasurements, ctx)
}

func newHostWithMeasurementGenerator(gen generator, ctx *HostContext) Host {
	sm := gen(ctx)

	region := randomRegionSliceChoice(regions)

	h := Host{
		// Tag Values that are static throughout the life of a Host:
		Name:               fmt.Sprintf(hostFmt, ctx.id),
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
		GenericMetricCount:    ctx.metricCount,
		StartEpoch:            math.MaxUint64,
		EpochsToLive:          ctx.epochsToLive,
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
