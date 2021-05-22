package query

import (
	"fmt"
)

// Query is an interface used for encoding a benchmark query for different databases
type Query interface {
	Release()
	HumanLabelName() []byte
	HumanDescriptionName() []byte
	GetID() uint64
	SetID(uint64)
	fmt.Stringer
}
