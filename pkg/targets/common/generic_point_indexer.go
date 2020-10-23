package common

import (
	"github.com/timescale/tsbs/pkg/data"
	"hash"
	"hash/fnv"
)

// hashPropertySelectFn defines a function that
// for a data.LoadedPoint return a byte array generated
// from the point properties that will be
// used to calculate the hash
type hashPropertySelectFn func(point *data.LoadedPoint) []byte

// GenericPointIndexer implements the targets.PointIndexer
// where the input for the hash function is provided
// as an input function
type GenericPointIndexer struct {
	propertySelector hashPropertySelectFn
	hasher           hash.Hash32
	maxPartitions    uint
}

func NewGenericPointIndexer(maxPartitions uint, propertySelector hashPropertySelectFn) *GenericPointIndexer {
	return &GenericPointIndexer{
		hasher:           fnv.New32a(),
		propertySelector: propertySelector,
		maxPartitions:    maxPartitions,
	}
}

func (g *GenericPointIndexer) GetIndex(point data.LoadedPoint) uint {
	g.hasher.Reset()
	g.hasher.Write(g.propertySelector(&point))
	return uint(g.hasher.Sum32()) % g.maxPartitions
}
