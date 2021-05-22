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
	"io"
	"sort"

	"github.com/golang/protobuf/proto"
	"github.com/prometheus/common/model"
	"github.com/timescale/promscale/pkg/prompb"
	"github.com/timescale/tsbs/pkg/data"
)

const serializerVersion uint64 = 1

type void struct{}

var supportedVersions = map[uint64]void{1: {}}

type Serializer struct {
	headerWritten bool
}

func (ps *Serializer) writeHeader(w io.Writer) (int, error) {
	if ps.headerWritten {
		return 0, nil
	}
	var versionBuf [binary.MaxVarintLen32]byte
	bytesWriter := binary.PutUvarint(versionBuf[:], serializerVersion)
	x, err := w.Write(versionBuf[:bytesWriter])
	if err != nil {
		return 0, fmt.Errorf("error writing file headear: %v", err)
	}
	ps.headerWritten = true
	return x, nil
}

// Serialize point into our custom binary format
func (ps *Serializer) Serialize(p *data.Point, w io.Writer) error {
	if !ps.headerWritten {
		if _, err := ps.writeHeader(w); err != nil {
			return err
		}
	}
	series := make([]prompb.TimeSeries, len(p.FieldKeys()))
	err := convertToPromSeries(p, series)
	if err != nil {
		return fmt.Errorf("could not serialize point\n%v", err)
	}
	for _, ts := range series {
		protoBytes, err := proto.Marshal(&ts)
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
func convertToPromSeries(p *data.Point, buffer []prompb.TimeSeries) error {
	bufLen := len(buffer)
	requiredPlaces := len(p.FieldKeys())
	if requiredPlaces > bufLen {
		return fmt.Errorf("supplied buffer has insufficient space; need %d; got %d",
			requiredPlaces, bufLen,
		)
	}
	tagKeys := p.TagKeys()
	tagValues := p.TagValues()
	fieldKeys := p.FieldKeys()
	fieldValues := p.FieldValues()
	// length is number of tagKeys plus the metric name
	labels := make([]prompb.Label, len(tagKeys)+1)
	for i := range tagKeys {
		label := prompb.Label{
			Name:  string(tagKeys[i]),
			Value: tagValues[i].(string),
		}
		labels[i] = label
	}
	labels[len(labels)-1] = prompb.Label{
		Name:  model.MetricNameLabel,
		Value: "",
	}
	sort.Slice(labels, func(i, j int) bool {
		return labels[i].Name < labels[j].Name
	})
	metricIndex := sort.Search(len(labels), func(i int) bool {
		return labels[i].Name >= model.MetricNameLabel
	})

	tsMs := p.TimestampInUnixMs()
	for i := range fieldKeys {
		myLabels := labels
		if i+1 < len(fieldKeys) {
			myLabels = make([]prompb.Label, len(labels))
			copy(myLabels, labels)
		}
		myLabels[metricIndex] = prompb.Label{
			Name:  model.MetricNameLabel,
			Value: string(fieldKeys[i]),
		}
		ts := prompb.TimeSeries{
			Labels:  myLabels,
			Samples: []prompb.Sample{{Value: getFloat64(fieldValues[i]), Timestamp: tsMs}},
		}
		buffer[i] = ts
	}
	return nil
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

// Iterator iterates over binary data and enables lazy access over protobuf messages
type Iterator struct {
	reader    *bufio.Reader
	processed uint64 // number of processed protobuf messages
}

// NewPrometheusIterator creates iterator and reads version information from underlying reader
func NewPrometheusIterator(reader *bufio.Reader) (*Iterator, error) {
	version, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, fmt.Errorf("error while reading file version: %v", err)
	}
	if _, exists := supportedVersions[version]; !exists {
		return nil, fmt.Errorf("unsupported version number: %d", version)
	}
	return &Iterator{reader: reader}, nil
}

// HasNext returns true if there are more protobuf messages to read
func (pi *Iterator) HasNext() bool {
	bytes, err := pi.reader.Peek(1)
	if err != nil {
		return false
	}
	return len(bytes) > 0
}

// Next returns next protobuf message
func (pi *Iterator) Next() (*prompb.TimeSeries, error) {
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
