package main

import (
	"bytes"
	"sync"

	flatbuffers "github.com/google/flatbuffers/go"
)

var fbBuilderPool = &sync.Pool{
	New: func() interface{} {
		return flatbuffers.NewBuilder(0)
	},
}
var bufPool = &sync.Pool{
	New: func() interface{} {
		return []byte{}
	},
}
var inlineTagsPool = &sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}
var bufPool8 = &sync.Pool{
	New: func() interface{} {
		return make([]byte, 8)
	},
}
