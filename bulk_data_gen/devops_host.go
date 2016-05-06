package main
import (
	"fmt"
	"math/rand"
	"time"
)

const NHostSims = 9

// Count of choices for auto-generated tag values:
const (
	MachineRackChoicesPerDatacenter = 100
	MachineServiceChoices           = 20
	MachineServiceVersionChoices    = 2
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
	// The duration of a log epoch.
	EpochDuration = 10 * time.Second

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
)




// Type Host models a machine being monitored by Telegraf.
type Host struct {
	SimulatedMeasurements []SimulatedMeasurement

	// These are all assigned once, at Host creation:
	Name, Region, Datacenter, Rack, OS, Arch          []byte
	Team, Service, ServiceVersion, ServiceEnvironment []byte
}

func NewHostMeasurements(start time.Time) []SimulatedMeasurement {
	sm := []SimulatedMeasurement{
		NewKernelMeasurement(start),
		NewCPUMeasurement(start),
		NewMemMeasurement(start),
		NewNetMeasurement(start),
		NewDiskMeasurement(start),
		NewDiskIOMeasurement(start),
		NewNginxMeasurement(start),
		NewRedisMeasurement(start),
		NewPostgresqlMeasurement(start),
	}

	if len(sm) != NHostSims {
		panic("logic error: incorrect number of measurements")
	}
	return sm
}

func NewHost(i int, start time.Time) Host {
	sm := NewHostMeasurements(start)

	region := &Regions[rand.Intn(len(Regions))]
	rackId := rand.Int63n(MachineRackChoicesPerDatacenter)
	serviceId := rand.Int63n(MachineServiceChoices)
	serviceVersionId := rand.Int63n(MachineServiceVersionChoices)
	serviceEnvironment := randChoice(MachineServiceEnvironmentChoices)

	h := Host{
		// Tag Values that are static throughout the life of a Host:
		Name:               []byte(fmt.Sprintf("host_%d", i)),
		Region:             []byte(fmt.Sprintf("%s", region.Name)),
		Datacenter:         randChoice(region.Datacenters),
		Rack:               []byte(fmt.Sprintf("%d", rackId)),
		Arch:               randChoice(MachineArchChoices),
		OS:                 randChoice(MachineOSChoices),
		Service:            []byte(fmt.Sprintf("%d", serviceId)),
		ServiceVersion:     []byte(fmt.Sprintf("%d", serviceVersionId)),
		ServiceEnvironment: serviceEnvironment,
		Team:               randChoice(MachineTeamChoices),

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


func randChoice(choices [][]byte) []byte {
	idx := rand.Int63n(int64(len(choices)))
	return choices[idx]
}
