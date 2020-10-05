package main

import (
	"github.com/timescale/tsbs/load"
)

type batch struct {
	points  []*point
	metrics uint64
}

func (b *batch) Len() int {
	return len(b.points)
}

func (b *batch) Append(item *load.Point) {
	p := item.Data.(*point)
	b.points = append(b.points, p)
}

type factory struct{}

func (f *factory) New() load.Batch {
	return &batch{}
}
