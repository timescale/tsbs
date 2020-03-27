package common

import (
	"fmt"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/pkg/targets/constants"
	"strings"
	"time"
)

// Generator is a single entity which generates data from its respective measurements.
type Generator interface {
	Measurements() []SimulatedMeasurement
	Tags() []Tag
	TickAll(d time.Duration)
}

// Tag is a key-value pair of information which is used to tag a generator
type Tag struct {
	Key   []byte
	Value interface{}
}

const (
	errMaxMetricCountValue = "max metric count per host has to be greater than 0"
	errLogIntervalZero     = "cannot have log interval of 0"
	defaultLogInterval     = 10 * time.Second
)

// DataGeneratorConfig is the GeneratorConfig that should be used with a
// DataGenerator. It includes all the fields from a BaseConfig, as well as some
// options that are specific to generating the data for database write operations,
// such as the initial scale and how spaced apart data points should be in time.
type DataGeneratorConfig struct {
	BaseConfig            `yaml:"base"`
	Limit                 uint64        `yaml:"max-data-points" mapstructure:"max-data-points"`
	InitialScale          uint64        `yaml:"initial-scale" mapstructure:"initial-scale" `
	LogInterval           time.Duration `yaml:"log-interval" mapstructure:"log-interval"`
	InterleavedGroupID    uint          `yaml:"interleaved-generation-group-id" mapstructure:"interleaved-generation-group-id"`
	InterleavedNumGroups  uint          `yaml:"interleaved-generation-groups" mapstructure:"interleaved-generation-groups"`
	MaxMetricCountPerHost uint64        `yaml:"max-metric-count" mapstructure:"max-metric-count"`
}

// Validate checks that the values of the DataGeneratorConfig are reasonable.
func (c *DataGeneratorConfig) Validate() error {
	err := c.BaseConfig.Validate()
	if err != nil {
		return err
	}

	if c.InitialScale == 0 {
		c.InitialScale = c.BaseConfig.Scale
	}

	if c.LogInterval == 0 {
		return fmt.Errorf(errLogIntervalZero)
	}

	err = utils.ValidateGroups(c.InterleavedGroupID, c.InterleavedNumGroups)

	if c.Use == UseCaseDevopsGeneric && c.MaxMetricCountPerHost < 1 {
		return fmt.Errorf(errMaxMetricCountValue)
	}

	return err
}

func (c *DataGeneratorConfig) AddToFlagSet(fs *pflag.FlagSet) {
	c.BaseConfig.AddToFlagSet(fs)
	fs.Uint64("max-data-points", 0, "Limit the number of data points to generate, 0 = no limit")
	fs.Uint64("initial-scale", 0, "Initial scaling variable specific to the use case (e.g., devices in 'devops'). 0 means to use -scale value")
	fs.Duration("log-interval", defaultLogInterval, "Duration between data points")

	fs.Uint("interleaved-generation-group-id", 0,
		"Group (0-indexed) to perform round-robin serialization within. Use this to scale up data generation to multiple processes.")
	fs.Uint("interleaved-generation-groups", 1,
		"The number of round-robin serialization groups. Use this to scale up data generation to multiple processes.")
	fs.Uint64("max-metric-count", 100, "Max number of metric fields to generate per host. Used only in devops-generic use-case")
}

const defaultTimeStart = "2016-01-01T00:00:00Z"

const defaultTimeEnd = "2016-01-02T00:00:00Z"

const errBadFormatFmt = "invalid format specified: '%v'"

const errBadUseFmt = "invalid use case specified: '%v'"

// GeneratorConfig is an interface that defines a configuration that is used
// by Generators to govern their behavior. The interface methods provide a way
// to use the GeneratorConfig with the command-line via flag.FlagSet and
// a method to validate the config is actually valid.
type GeneratorConfig interface {
	// AddToFlagSet adds all the config options to a FlagSet, for easy use with CLIs
	AddToFlagSet(fs *pflag.FlagSet)
	// Validate checks that configuration is valid and ready to be consumed by a Generator
	Validate() error
}

// BaseConfig is a data struct that includes the common flags or configuration
// options shared across different types of Generators. These include things like
// the data format (i.e., which database system is this for), a PRNG seed, etc.
type BaseConfig struct {
	Format string `yaml:"format,omitempty" mapstructure:"format,omitempty"`
	Use    string `yaml:"use-case" mapstructure:"use-case"`

	Scale uint64

	TimeStart string `yaml:"timestamp-start" mapstructure:"timestamp-start"`
	TimeEnd   string `yaml:"timestamp-end" mapstructure:"timestamp-end"`

	Seed  int64
	Debug int    `yaml:"debug,omitempty" mapstructure:"debug,omitempty"`
	File  string `yaml:"file,omitempty" mapstructure:"file,omitempty"`
}

func (c *BaseConfig) AddToFlagSet(fs *pflag.FlagSet) {
	fs.String("format", "", fmt.Sprintf("Format to generate. (choices: %s)", strings.Join(constants.SupportedFormats(), ", ")))
	fs.String("use-case", "", fmt.Sprintf("Use case to generate."))

	fs.Uint64("scale", 1, "Scaling value specific to use case (e.g., devices in 'devops').")

	fs.String("timestamp-start", defaultTimeStart, "Beginning timestamp (RFC3339).")
	fs.String("timestamp-end", defaultTimeEnd, "Ending timestamp (RFC3339).")

	fs.Int64("seed", 0, "PRNG seed (default: 0, which uses the current timestamp)")
	fs.Int("debug", 0, "Control level of debug output")
	fs.String("file", "", "Write the output to this path")
}

func (c *BaseConfig) Validate() error {
	if c.Scale == 0 {
		return fmt.Errorf(ErrScaleIsZero)
	}

	if c.Seed == 0 {
		c.Seed = int64(time.Now().Nanosecond())
	}

	if !utils.IsIn(c.Format, constants.SupportedFormats()) {
		return fmt.Errorf(errBadFormatFmt, c.Format)
	}

	if !utils.IsIn(c.Use, UseCaseChoices) {
		return fmt.Errorf(errBadUseFmt, c.Use)
	}

	return nil
}

const ErrScaleIsZero = "scale cannot be 0"
