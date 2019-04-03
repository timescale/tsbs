package inputs

import (
	"flag"
	"fmt"
	"strings"
	"time"
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
	AddToFlagSet(fs *flag.FlagSet)
	// Validate checks that configuration is valid and ready to be consumed by a Generator
	Validate() error
}

// BaseConfig is a data struct that includes the common flags or configuration
// options shared across different types of Generators. These include things like
// the data format (i.e., which database system is this for), a PRNG seed, etc.
type BaseConfig struct {
	Format string
	Use    string

	Scale uint64
	Limit uint64

	TimeStart string
	TimeEnd   string

	Seed    int64
	Verbose bool
	File    string
}

func (c *BaseConfig) AddToFlagSet(fs *flag.FlagSet) {
	fs.StringVar(&c.Format, "format", "", fmt.Sprintf("Format to generate. (choices: %s)", strings.Join(ValidFormats(), ", ")))
	fs.StringVar(&c.Use, "use-case", "", fmt.Sprintf("Use case to generate."))
	fs.StringVar(&c.File, "file", "", "Write the output to this path")

	fs.StringVar(&c.TimeStart, "timestamp-start", defaultTimeStart, "Beginning timestamp (RFC3339).")
	fs.StringVar(&c.TimeEnd, "timestamp-end", defaultTimeEnd, "Ending timestamp (RFC3339).")

	fs.Uint64Var(&c.Scale, "scale", 1, "Scaling value specific to use case (e.g., devices in 'devops').")
	fs.Int64Var(&c.Seed, "seed", 0, "PRNG seed (default: 0, which uses the current timestamp)")

	fs.BoolVar(&c.Verbose, "verbose", false, "Show verbose output")
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
