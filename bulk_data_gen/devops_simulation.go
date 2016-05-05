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


//func newCPUDistributions(count int) []Distribution {
//		for i := 0; i < len(CPUFieldKeys); i++ {
//			n := host.CPUFieldDistributions[i].Get()
//			p.AppendField(CPUFieldKeys[i], n)
//		}
//	dd := make([]Distribution, count)
//	for i := 0; i < len(dd); i++ {
//		dd[i] = &ClampedRandomWalkDistribution{
//			State: rand.Float64() * 100.0,
//			Min:   0.0,
//			Max:   100.0,
//			Step: &NormalDistribution{
//				Mean:   0.0,
//				StdDev: 1.0,
//			},
//		}
//	}
//	return dd
//}
