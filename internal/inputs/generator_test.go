package inputs

import (
	"fmt"
	"testing"
)

func TestBaseConfigValidate(t *testing.T) {
	c := &BaseConfig{
		Scale:  1,
		Seed:   123,
		Format: FormatTimescaleDB,
		Use:    useCaseDevops,
	}

	// Test Scale validation
	err := c.Validate()
	if err != nil {
		t.Errorf("unexpected error with scale 1: %v", err)
	}

	c.Scale = 0
	err = c.Validate()
	if err == nil {
		t.Errorf("unexpected lack of error for scale of 0")
	} else if got := err.Error(); got != ErrScaleIsZero {
		t.Errorf("incorrect error for scale of 0: got\n%s\nwant\n%s", got, ErrScaleIsZero)
	}
	c.Scale = 1

	// Test Seed validation
	err = c.Validate()
	if err != nil {
		t.Errorf("unexpected error with seed 123: %v", err)
	}
	if c.Seed != 123 {
		t.Errorf("seed was not 123 after validation")
	}

	c.Seed = 0
	err = c.Validate()
	if err != nil {
		t.Errorf("unexpected error with seed 0: %v", err)
	}
	if c.Seed == 0 {
		t.Errorf("seed was not set to nanosecond when 0")
	}

	// Test Format validation
	c.Format = FormatCassandra
	err = c.Validate()
	if err != nil {
		t.Errorf("unexpected error with Format '%s': %v", FormatCassandra, err)
	}

	c.Format = "unknown type"
	err = c.Validate()
	if err == nil {
		t.Errorf("unexpected lack of error for incorrect format")
	} else {
		want := fmt.Sprintf(errBadFormatFmt, "unknown type")
		if got := err.Error(); got != want {
			t.Errorf("incorrect error for incorrect format: got\n%v\nwant\n%v", got, want)
		}
	}
	c.Format = FormatTimescaleDB

	// Test Use validation
	c.Use = useCaseCPUOnly
	err = c.Validate()
	if err != nil {
		t.Errorf("unexpected error with Use '%s': %v", useCaseCPUOnly, err)
	}

	c.Use = "bad use"
	err = c.Validate()
	if err == nil {
		t.Errorf("unexpected lack of error for incorrect use")
	} else {
		want := fmt.Sprintf(errBadUseFmt, "bad use")
		if got := err.Error(); got != want {
			t.Errorf("incorrect error for incorrect format: got\n%v\nwant\n%v", got, want)
		}
	}
	c.Use = useCaseDevops
}
