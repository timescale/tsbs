package inputs

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/pflag"
)

// Error messages when using a GeneratorConfig
const (
	ErrScaleIsZero = "scale cannot be 0"

	errBadFormatFmt = "invalid format specified: '%v'"
	errBadUseFmt    = "invalid use case specified: '%v'"
)

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
	Format string `mapstructure:"format"`
	Use    string `mapstructure:"use-case"`

	Scale uint64 `mapstructure:"scale"`

	TimeStart string `mapstructure:"timestamp-start"`
	TimeEnd   string `mapstructure:"timestamp-end"`

	Seed  int64  `mapstructure:"seed"`
	Debug int    `mapstructure:"debug"`
	File  string `mapstructure:"file"`
}

func (c *BaseConfig) AddToFlagSet(fs *pflag.FlagSet) {
	fs.String("format", "", fmt.Sprintf("Format to generate. (choices: %s)", strings.Join(formats, ", ")))
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

	if !isIn(c.Format, formats) {
		return fmt.Errorf(errBadFormatFmt, c.Format)
	}

	if !isIn(c.Use, useCaseChoices) {
		return fmt.Errorf(errBadUseFmt, c.Use)
	}

	return nil
}

// Generator is an interface that defines a type that generates inputs to other
// TSBS tools. Examples include DataGenerator which creates database data that
// gets inserted and stored, or QueryGenerator which creates queries that are
// used to test with.
type Generator interface {
	Generate(GeneratorConfig) error
}
