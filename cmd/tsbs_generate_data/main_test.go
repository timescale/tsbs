package main

import (
	"bufio"
	"bytes"
	"testing"

	"bitbucket.org/440-labs/tsbs/cmd/tsbs_generate_data/devops"
	"bitbucket.org/440-labs/tsbs/cmd/tsbs_generate_data/serialize"
)

func TestValidateFormat(t *testing.T) {
	for _, f := range formatChoices {
		if !validateFormat(f) {
			t.Errorf("format '%s' did not return true when it should", f)
		}
	}
	if validateFormat("incorrect format!") {
		t.Errorf("validateFormat returned true for invalid format")
	}
}

func TestGetConfig(t *testing.T) {
	cfg := getConfig(useCaseDevops)
	switch got := cfg.(type) {
	case *devops.DevopsSimulatorConfig:
	default:
		t.Errorf("use case '%s' does not run the right type: got %T", useCaseDevops, got)
	}

	cfg = getConfig(useCaseCPUOnly)
	switch got := cfg.(type) {
	case *devops.CPUOnlySimulatorConfig:
	default:
		t.Errorf("use case '%s' does not run the right type: got %T", useCaseDevops, got)
	}

	cfg = getConfig(useCaseCPUSingle)
	switch got := cfg.(type) {
	case *devops.CPUOnlySimulatorConfig:
	default:
		t.Errorf("use case '%s' does not run the right type: got %T", useCaseDevops, got)
	}

	fatalCalled := false
	fatal = func(f string, args ...interface{}) {
		fatalCalled = true
	}
	cfg = getConfig("bogus config")
	if !fatalCalled {
		t.Errorf("fatal not called on bogus use case")
	}
	if cfg != nil {
		t.Errorf("got a non-nil config for bogus use case: got %T", cfg)
	}
}

func TestGetSerializer(t *testing.T) {
	cfg := getConfig(useCaseCPUOnly)
	sim := cfg.ToSimulator(logInterval)
	buf := bytes.NewBuffer(make([]byte, 1024))
	out := bufio.NewWriter(buf)
	defer out.Flush()

	s := getSerializer(sim, formatCassandra, out)
	switch got := s.(type) {
	case *serialize.CassandraSerializer:
	default:
		t.Errorf("format '%s' does not run the right serializer: got %T", formatCassandra, got)
	}

	s = getSerializer(sim, formatInflux, out)
	switch got := s.(type) {
	case *serialize.InfluxSerializer:
	default:
		t.Errorf("format '%s' does not run the right serializer: got %T", formatInflux, got)
	}

	s = getSerializer(sim, formatMongo, out)
	switch got := s.(type) {
	case *serialize.MongoSerializer:
	default:
		t.Errorf("format '%s' does not run the right serializer: got %T", formatMongo, got)
	}

	s = getSerializer(sim, formatTimescaleDB, out)
	switch got := s.(type) {
	case *serialize.TimescaleDBSerializer:
	default:
		t.Errorf("format '%s' does not run the right serializer: got %T", formatTimescaleDB, got)
	}

	fatalCalled := false
	fatal = func(f string, args ...interface{}) {
		fatalCalled = true
	}
	s = getSerializer(sim, "bogus format", out)
	if !fatalCalled {
		t.Errorf("fatal not called on bogus format")
	}
	if s != nil {
		t.Errorf("got a non-nil config for bogus format: got %T", cfg)
	}
}
