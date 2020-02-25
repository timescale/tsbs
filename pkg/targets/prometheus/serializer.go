package prometheus

// Prometheus serializer writes data into our custom binary format that looks like:
// <<header<version>>><<message_size><protobuf message>><<message_size><protobuf message>>...
// Since protobuf isn't self-delimiting to enable stream processing of really large
// generated files we needed to implement our own format using techniques decribed
// here https://developers.google.com/protocol-buffers/docs/techniques#streaming

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/timescale/tsbs/pkg/data"
	"io"

	"github.com/golang/protobuf/proto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/timescale/tsbs/pkg/data"
)

const serializerVersion uint64 = 1

type void struct{}

var supportedVersions = map[uint64]void{1: void{}}

type Serializer struct {
}

// NewPrometheusSerializer creates serializer. The serializer is stateful and shouldn't
// be reused across different writers
func NewPrometheusSerializer(writer io.Writer) (*Serializer, error) {
	ps := &Serializer{}
	_, err := ps.writeHeader(writer)
	if err != nil {
		return nil, fmt.Errorf("error writing file headear: %v", err)
	}
	return ps, err
}

func (ps *Serializer) writeHeader(w io.Writer) (int, error) {
	var versionBuf [binary.MaxVarintLen32]byte
	bytesWritter := binary.PutUvarint(versionBuf[:], serializerVersion)
	return w.Write(versionBuf[:bytesWritter])
}

// Serialize point into our custom binary format
func (ps *Serializer) Serialize(p *data.Point, w io.Writer) error {
	series := ps.convertToPromSeries(p)
	for _, ts := range series {
		protoBytes, err := proto.Marshal(ts)
		if err != nil {
			return err
		}
		var msgSizeBuf [binary.MaxVarintLen32]byte
		bytesWritten := binary.PutUvarint(msgSizeBuf[:], uint64(len(protoBytes)))
		_, err = w.Write(msgSizeBuf[:bytesWritten])
		if err != nil {
			return err
		}
		_, err = w.Write(protoBytes)
		if err != nil {
			return err
		}
	}
	return nil
}

// Each point field will become a new TimeSeries with added field key as a label
func (ps *Serializer) convertToPromSeries(p *data.Point) []*prompb.TimeSeries {
	tagKeys := p.TagKeys()
	tagValues := p.TagValues()
	fieldKeys := p.FieldKeys()
	fieldValues := p.FieldValues()
	labels := make([]prompb.Label, len(tagKeys))
	series := make([]*prompb.TimeSeries, len(fieldKeys))
	for i := range tagKeys {
		label := prompb.Label{
			Name:  string(tagKeys[i]),
			Value: tagValues[i].(string),
		}
		labels[i] = label
	}
	tsMs := p.Timestamp().UnixNano() / 1000000
	for i := range fieldKeys {
		metricNameLabel := prompb.Label{
			Name:  model.MetricNameLabel,
			Value: string(fieldKeys[i]),
		}
		allLabels := append(labels, metricNameLabel)

		ts := &prompb.TimeSeries{
			Labels:  allLabels,
			Samples: []prompb.Sample{prompb.Sample{Value: getFloat64(fieldValues[i]), Timestamp: tsMs}},
		}
		series[i] = ts
	}
	return series
}

func getFloat64(fieldValue interface{}) float64 {
	switch t := fieldValue.(type) {
	case int:
		return float64(fieldValue.(int))
	case int64:
		return float64(fieldValue.(int64))
	case float64:
		return fieldValue.(float64)
	default:
		panic(fmt.Sprintf("unsupported value type: %v", t))
	}
}

// PrometheusIterator iterates over binary data and enables lazy access over protobuf messages
type PrometheusIterator struct {
	reader    *bufio.Reader
	processed uint64 // number of processed protobuf messages
}

// NewPrometheusIterator creates iterator and reads version information from underlying reader
func NewPrometheusIterator(reader *bufio.Reader) (*PrometheusIterator, error) {
	version, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, fmt.Errorf("error while reading file version: %v", err)
	}
	if _, exists := supportedVersions[version]; !exists {
		return nil, fmt.Errorf("unsupported version number: %d", version)
	}
	return &PrometheusIterator{reader: reader}, nil
}

// HasNext returns true if there are more protobuf messages to read
func (pi *PrometheusIterator) HasNext() bool {
	bytes, err := pi.reader.Peek(1)
	if err != nil {
		return false
	}
	return len(bytes) > 0
}

// Next returns next protobuf message
func (pi *PrometheusIterator) Next() (*prompb.TimeSeries, error) {
	messageSize, err := binary.ReadUvarint(pi.reader)
	if err != nil {
		return nil, fmt.Errorf("error while reading message size")
	}

	messageBuf := make([]byte, messageSize)
	_, err = io.ReadFull(pi.reader, messageBuf)
	if err != nil {
		return nil, fmt.Errorf("error while reading protobuf message: %v", err)
	}
	ts := &prompb.TimeSeries{}
	err = proto.Unmarshal(messageBuf, ts)
	if err != nil {
		return nil, err
	}
	pi.processed++
	return ts, nil
}
