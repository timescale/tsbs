package usecases

import (
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/data/usecases/devops"
	"github.com/timescale/tsbs/pkg/data/usecases/iot"
	"reflect"
	"testing"
	"time"
)

const defaultLogInterval = 10 * time.Second

func TestGetSimulatorConfig(t *testing.T) {
	dgc := &common.DataGeneratorConfig{
		BaseConfig: common.BaseConfig{
			Scale:     1,
			TimeStart: "2020-01-01T00:00:00Z",
			TimeEnd:   "2020-01-01T00:00:01Z",
		},
		InitialScale: 1,
		LogInterval:  defaultLogInterval,
	}

	checkType := func(use string, want common.SimulatorConfig) {
		wantType := reflect.TypeOf(want)
		dgc.Use = use
		scfg, err := GetSimulatorConfig(dgc)
		if err != nil {
			t.Errorf("unexpected error with use case %s: %v", use, err)
		}
		if got := reflect.TypeOf(scfg); got != wantType {
			t.Errorf("use '%s' does not give right scfg: got %v want %v", use, got, wantType)
		}
	}

	checkType(common.UseCaseDevops, &devops.DevopsSimulatorConfig{})
	checkType(common.UseCaseIoT, &iot.SimulatorConfig{})
	checkType(common.UseCaseCPUOnly, &devops.CPUOnlySimulatorConfig{})
	checkType(common.UseCaseCPUSingle, &devops.CPUOnlySimulatorConfig{})

	dgc.Use = "bogus use case"
	_, err := GetSimulatorConfig(dgc)
	if err == nil {
		t.Errorf("unexpected lack of error for bogus use case")
	}
}
