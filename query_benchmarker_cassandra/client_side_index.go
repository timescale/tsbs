package main

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

// A ClientSideIndex wraps logic to translate a query into the cassandra keys
// needed to execute that query. After initialization, this type is read-only.
type ClientSideIndex struct {
	timeIntervalMapping map[TimeInterval]map[*Series]struct{}
	tagMapping          map[string]map[*Series]struct{}

	seriesCollection []Series
	seriesIds        []string
}

func NewClientSideIndex(seriesCollection []Series) *ClientSideIndex {
	if len(seriesCollection) == 0 {
		log.Fatal("logic error: no data to build ClientSideIndex")
	}

	bm := map[TimeInterval]map[*Series]struct{}{}

	for _, s := range seriesCollection {
		if _, ok := bm[s.TimeInterval]; !ok {
			bm[s.TimeInterval] = map[*Series]struct{}{}
		}
		if _, ok := bm[s.TimeInterval][&s]; !ok {
			bm[s.TimeInterval][&s] = struct{}{}
		}
	}

	tm := map[string]map[*Series]struct{}{}

	for _, s := range seriesCollection {
		for tag, _ := range s.Tags {
			if _, ok := tm[tag]; !ok {
				tm[tag] = map[*Series]struct{}{}
			}
			if _, ok := tm[tag][&s]; !ok {
				tm[tag][&s] = struct{}{}
			}
		}
	}

	seriesIds := make([]string, 0, len(seriesCollection))
	for _, s := range seriesCollection {
		seriesIds = append(seriesIds, s.Id)
	}

	return &ClientSideIndex{
		timeIntervalMapping: bm,
		tagMapping:        tm,
		seriesCollection:  seriesCollection,
		seriesIds:         seriesIds,
	}
}

func (csi *ClientSideIndex) CopyOfSeriesCollection() []Series {
	ret := make([]Series, len(csi.seriesCollection))
	copy(ret, csi.seriesCollection)
	return ret
}

// linear in the number of filters
func (csi *ClientSideIndex) SeriesSelector(q *HLQuery) []Series {
	// begin with a set of all possible Series:
	m := map[*Series]struct{}{}
	for _, s := range csi.seriesCollection {
		safeCopy := s
		m[&safeCopy] = struct{}{}
	}
	//fmt.Printf("m len: %d\n", len(m))

	// filter all Series against the predicate:
	ret := []Series{}
	for s := range m {
		if q.AppliesToSeries(s) {
			ret = append(ret, *s)
		}
	}
	return ret
}

// A Series maps to a time series in cassandra. All data in this type are just
// keys used in the database.
type Series struct {
	Table string // e.g. "series_bigint"
	Id    string // e.g. "cpu,hostname=host_0,region=eu-central-1#usage_idle#2016-01-01"

	// parsed fields
	Measurement  string              // e.g. "cpu"
	Tags         map[string]struct{} // e.g. {"hostname": "host_3"}
	Field        string              // e.g. "usage_idle"
	TimeInterval TimeInterval        // (UTC) e.g. "2016-01-01"
}

func NewSeries(table, id string) Series {
	s := Series{
		Table: table,
		Id:    id,
	}

	s.parse()
	return s
}

func (s *Series) parse() {
	// expected format:
	// cpu,hostname=host_0,region=eu-central-1,datacenter=eu-central-1a,rack=42,os=Ubuntu16.10,arch=x64,team=CHI,service=19,service_version=1,service_environment=staging#usage_idle#2016-01-01
	sections := strings.Split(s.Id, "#")
	if len(sections) != 3 {
		//fmt.Println(s.table)
		log.Fatal("logic error: invalid series id")
	}
	measurementAndTags := strings.Split(sections[0], ",")

	// parse measurement:
	s.Measurement = measurementAndTags[0]

	// parse tags:
	tags := map[string]struct{}{}
	for _, tag := range measurementAndTags[1:] {
		if _, ok := tags[tag]; ok {
			log.Fatal("logic error: duplicate tag")
		}

		tags[tag] = struct{}{}
	}
	s.Tags = tags

	// parse field name:
	s.Field = sections[1]

	// parse time interval:
	start, err := time.Parse("2006-01-02", sections[2])
	if err != nil {
		log.Fatal("bad time bucket parse in pre-existing database series")
	}
	end := start.Add(BucketDuration)
	s.TimeInterval = TimeInterval{start, end}
}

func (s *Series) MatchesTimeInterval(ti *TimeInterval) bool {
	return s.TimeInterval.Overlap(ti)
}

func (s *Series) MatchesMeasurementName(m string) bool {
	return s.Measurement == m
}

func (s *Series) MatchesFieldName(f string) bool {
	return s.Field == f
}

func (s *Series) MatchesTagFilters(tags []TagFilter) bool {
	for _, tag := range tags {
		tagStr := string(tag)
		_, ok := s.Tags[tagStr]
		if !ok {
			return false
		}
	}
	return true
}

func (q *HLQuery) AppliesToSeries(s *Series) bool {
	panic("unreachable")
	return true
}

func fetchSeriesCollection(daemonUrl string) []Series {
	cluster := gocql.NewCluster(daemonUrl)
	cluster.Keyspace = "measurements"
	cluster.Consistency = gocql.One
	cluster.ProtoVersion = 4
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	seriesCollection := []Series{}

	for _, tableName := range blessedTables {
		var seriesId string
		iter := session.Query(fmt.Sprintf(`SELECT DISTINCT series_id FROM %s`, tableName)).Iter()
		for iter.Scan(&seriesId) {
			//fmt.Println(tableName, seriesId)
			s := NewSeries(tableName, seriesId)
			seriesCollection = append(seriesCollection, s)

		}
		if err := iter.Close(); err != nil {
			log.Fatal(err)
		}
	}
	return seriesCollection
}
