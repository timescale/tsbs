package source

import (
	"fmt"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets/constants"
	"testing"
	"time"
)

const (
	errInvalidGroupsFmt = "incorrect interleaved groups configuration: id %d >= total groups %d"
	errTotalGroupsZero  = "incorrect interleaved groups configuration: total groups = 0"
	errLogIntervalZero  = "cannot have log interval of 0"
)

func TestDataGeneratorConfigValidate(t *testing.T) {
	c := &common.DataGeneratorConfig{
		BaseConfig: common.BaseConfig{
			Seed:   123,
			Format: constants.FormatTimescaleDB,
			Use:    common.UseCaseDevops,
			Scale:  10,
		},
		LogInterval:          time.Second,
		InitialScale:         0,
		InterleavedGroupID:   0,
		InterleavedNumGroups: 1,
	}

	// Test base validation
	err := c.Validate()
	if err != nil {
		t.Errorf("unexpected error for correct config: %v", err)
	}

	c.Format = "bad format"
	err = c.Validate()
	if err == nil {
		t.Errorf("unexpected lack of error for bad format")
	}
	c.Format = constants.FormatTimescaleDB

	// Test InitialScale validation
	c.InitialScale = 0
	err = c.Validate()
	if err != nil {
		t.Errorf("unexpected error for InitialScale of 0: %v", err)
	}
	if c.InitialScale != c.Scale {
		t.Errorf("InitialScale not set correctly for 0: got %d want %d", c.InitialScale, c.Scale)
	}

	c.InitialScale = 5
	err = c.Validate()
	if err != nil {
		t.Errorf("unexpected error for InitialScale of 5: %v", err)
	}
	if c.InitialScale != 5 {
		t.Errorf("InitialScale not set correctly for 0: got %d want %d", c.InitialScale, 5)
	}

	// Test LogInterval validation
	c.LogInterval = 0
	err = c.Validate()
	if err == nil {
		t.Errorf("unexpected lack of error for 0 log interval")
	} else if got := err.Error(); got != errLogIntervalZero {
		t.Errorf("incorrect error for 0 log interval: got\n%s\nwant\n%s", got, errLogIntervalZero)
	}
	c.LogInterval = time.Second

	// Test groups validation
	c.InterleavedNumGroups = 0
	err = c.Validate()
	if err == nil {
		t.Errorf("unexpected lack of error for 0 groups")
	} else if got := err.Error(); got != errTotalGroupsZero {
		t.Errorf("incorrect error for 0 groups: got\n%s\nwant\n%s", got, errTotalGroupsZero)
	}
	c.InterleavedNumGroups = 1

	c.InterleavedGroupID = 2
	err = c.Validate()
	if err == nil {
		t.Errorf("unexpected lack of error for group id > num groups")
	} else {
		want := fmt.Sprintf(errInvalidGroupsFmt, 2, 1)
		if got := err.Error(); got != want {
			t.Errorf("incorrect error for group id > num groups: got\n%s\nwant\n%s", got, want)
		}
	}
}
