package inputs

import (
	"github.com/timescale/tsbs/pkg/data/usecases/common"
)

// Generator is an interface that defines a type that generates inputs to other
// TSBS tools. Examples include DataGenerator which creates database data that
// gets inserted and stored, or QueryGenerator which creates queries that are
// used to test with.
type Generator interface {
	Generate(common.GeneratorConfig) error
}
