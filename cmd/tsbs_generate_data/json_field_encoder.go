package main

import (
	"bytes"
	"encoding/json"
	"math"
)

//customised json encoder to turn fields and values into json
//for postgres jsonb. Not general-purpose, created to be more
//efficient than standard go json encoder for this purpose
type jsonFieldEncoder struct {
	buf     *bytes.Buffer
	encoder *json.Encoder
	visited bool
}

func newJSONFieldEncoder(buf *bytes.Buffer) *jsonFieldEncoder {
	encoder := json.NewEncoder(buf)
	return &jsonFieldEncoder{buf, encoder, false}
}

func (enc *jsonFieldEncoder) Start() error {
	enc.visited = false
	err := enc.buf.WriteByte('{')
	if err != nil {
		return err
	}
	return nil
}

func (enc *jsonFieldEncoder) AddField(name string, value interface{}) error {
	enc.visited = true
	enc.encoder.Encode(name)
	enc.buf.Truncate(enc.buf.Len() - 1) //delete the newline
	err := enc.buf.WriteByte(':')
	if err != nil {
		return err
	}

	floatValue, ok := value.(float64)
	if ok {
		if math.IsNaN(floatValue) {
			value = "NaN"
		}
		if math.IsInf(floatValue, 1) {
			value = "Infinity"
		}
		if math.IsInf(floatValue, -1) {
			value = "-Infinity"
		}
	}
	enc.encoder.Encode(value)
	enc.buf.Truncate(enc.buf.Len() - 1) //delete the newline
	err = enc.buf.WriteByte(',')
	if err != nil {
		return err
	}
	return nil

}

func (enc *jsonFieldEncoder) End() error {
	if enc.visited {
		enc.buf.Truncate(enc.buf.Len() - 1) //delete the last ,
	}
	err := enc.buf.WriteByte('}')
	if err != nil {
		return err
	}
	return nil
}
