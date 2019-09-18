package common

import (
	"fmt"
	"math/rand"
	"reflect"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	internalutils "github.com/timescale/tsbs/internal/utils"
)

const (
	errMoreItemsThanScale = "cannot get random permutation with more items than scale"
)

// Core is the common component of all generators for all systems
type Core struct {
	// Interval is the entire time range of the dataset
	Interval *internalutils.TimeInterval

	// Scale is the cardinality of the dataset in terms of devices/hosts
	Scale int
}

// NewCore returns a new Core for the given time range and cardinality
func NewCore(start, end time.Time, scale int) (*Core, error) {
	ti, err := internalutils.NewTimeInterval(start, end)
	if err != nil {
		return nil, err
	}

	return &Core{Interval: ti, Scale: scale}, nil
}

// PanicUnimplementedQuery generates a panic for the provided query generator.
func PanicUnimplementedQuery(dg utils.QueryGenerator) {
	panic(fmt.Sprintf("database (%v) does not implement query", reflect.TypeOf(dg)))
}

// GetRandomSubsetPerm returns a subset of numItems of a permutation of numbers from 0 to totalNumbers,
// e.g., 5 items out of 30. This is an alternative to rand.Perm and then taking a sub-slice,
// which used up a lot more memory and slowed down query generation significantly.
// The subset of the permutation should have no duplicates and thus, can not be longer that original set
// Ex.: 12, 7, 25 for numItems=3 and totalItems=30 (3 out of 30)
func GetRandomSubsetPerm(numItems int, totalItems int) ([]int, error) {
	if numItems > totalItems {
		// Cannot make a subset longer than the original set
		return nil, fmt.Errorf(errMoreItemsThanScale)
	}

	seen := map[int]bool{}
	res := make([]int, numItems)
	for i := 0; i < numItems; i++ {
		for {
			n := rand.Intn(totalItems)
			// Keep iterating until a previously unseen int is found
			if !seen[n] {
				seen[n] = true
				res[i] = n
				break
			}
		}
	}
	return res, nil
}
