package timestream

import (
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"log"
	"strconv"
	"time"
)

type simulatorDataSource struct {
	_headers     *common.GeneratedDataHeaders
	simulator    common.Simulator
	useCurrentTs bool
}

func (s *simulatorDataSource) NextItem() data.LoadedPoint {
	if s._headers == nil {
		log.Fatal("headers not read before starting to read points")
		return data.LoadedPoint{}
	}
	newSimulatorPoint := data.NewPoint()
	var write bool
	for !s.simulator.Finished() {
		write = s.simulator.Next(newSimulatorPoint)
		if write {
			break
		}
		newSimulatorPoint.Reset()
	}
	if s.simulator.Finished() || !write {
		return data.LoadedPoint{}
	}
	timeUnixNano := s.prepareTimestamp(newSimulatorPoint.Timestamp())
	return data.NewLoadedPoint(&deserializedPoint{
		timeUnixNano: timeUnixNano,
		table:        string(newSimulatorPoint.MeasurementName()),
		tags:         tagsToStringArr(newSimulatorPoint.TagValues()),
		tagKeys:      tagKeysToStringArr(newSimulatorPoint.TagKeys()),
		fields:       fieldsToStringArr(newSimulatorPoint.FieldValues()),
	})
}

func (s *simulatorDataSource) prepareTimestamp(pointTs *time.Time) string {
	var ts time.Time
	if !s.useCurrentTs {
		ts = *pointTs
	} else {
		ts = time.Now()
	}
	return strconv.FormatInt(ts.UnixNano(), 10)
}

func (s *simulatorDataSource) Headers() *common.GeneratedDataHeaders {
	if s._headers != nil {
		return s._headers
	}

	s._headers = s.simulator.Headers()
	return s._headers
}

func tagsToStringArr(tagValues []interface{}) []string {
	tagsAsStr := make([]string, len(tagValues))
	for i, tag := range tagValues {
		var buf []byte
		tagsAsStr[i] = string(serialize.FastFormatAppend(tag, buf))
	}
	return tagsAsStr
}

func tagKeysToStringArr(tagKeys [][]byte) []string {
	tagsAsStr := make([]string, len(tagKeys))
	for i, tag := range tagKeys {
		tagsAsStr[i] = string(tag)
	}
	return tagsAsStr
}

func fieldsToStringArr(fieldValues []interface{}) []*string {
	fieldsAsStr := make([]*string, len(fieldValues))
	for i, field := range fieldValues {
		var buf []byte
		var str string
		str = string(serialize.FastFormatAppend(field, buf))
		fieldsAsStr[i] = &str
	}
	return fieldsAsStr
}
