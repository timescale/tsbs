package main

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

type timeBucket string

func newTimeBuckets(a, b time.Time) []timeBucket {
	ret := []timeBucket{}

	start := a.UnixNano()
	// round down to the first bucket:
	start -= (start % timeBucketInterval.Nanoseconds())

	// first window is always present:
	tb := timeBucket(time.Unix(0, start).Format("2016-01-02"))
	ret = append(ret, tb)
	start += timeBucketInterval.Nanoseconds()

	// populate remaining windows, if needed:
	end := b.UnixNano()
	for start < end {
		tb := timeBucket(time.Unix(0, start).Format("2016-01-02"))
		ret = append(ret, tb)

		start += timeBucketInterval.Nanoseconds()
	}

	return ret

}

// A ClientSideIndex wraps logic to translate a query into the cassandra keys
// needed to execute that query.
type ClientSideIndex struct {
	timeBucketMapping map[timeBucket]map[*series]struct{}
	tagMapping map[string]map[*series]struct{}

	seriesCollection []series
	seriesIds []string
}

func NewClientSideIndex(seriesCollection []series) *ClientSideIndex {
	if len(seriesCollection) == 0 {
		log.Fatal("logic error: no data to build ClientSideIndex")
	}

	bm := map[timeBucket]map[*series]struct{}{}

	for _, s := range seriesCollection {
		if _, ok := bm[s.timeBucket]; !ok {
			bm[s.timeBucket] = map[*series]struct{}{}
		}
		if _, ok := bm[s.timeBucket][&s]; !ok {
			bm[s.timeBucket][&s] = struct{}{}
		}
	}

	tm := map[string]map[*series]struct{}{}

	for _, s := range seriesCollection {
		for tag, _ := range s.tags {
			if _, ok := tm[tag]; !ok {
				tm[tag] = map[*series]struct{}{}
			}
			if _, ok := tm[tag][&s]; !ok {
				tm[tag][&s] = struct{}{}
			}
		}
	}

	seriesIds := make([]string, 0, len(seriesCollection))
	for _, s := range seriesCollection {
		seriesIds = append(seriesIds, s.id)
	}


	return &ClientSideIndex{
		timeBucketMapping: bm,
		tagMapping: tm,
		seriesCollection:  seriesCollection,
		seriesIds: seriesIds,
	}
}

// linear in the number of filters
func (csi *ClientSideIndex) SeriesSelector(q *Query) []series {
	// begin with a set of all possible series:
	m := map[*series]struct{}{}
	for _, s := range csi.seriesCollection {
		safeCopy := s
		m[&safeCopy] = struct{}{}
	}
	fmt.Printf("m len: %d\n", len(m))

	// make an ad-hoc predicate to indicate if a series matches this query:
	pred := func(s *series) bool {
		if q.MeasurementName != s.measurement {
			//fmt.Printf("pred false (measurement): %s != %s\n", q.MeasurementName, s.measurement)
			return false
		}
		if q.FieldName != s.field {
			//fmt.Printf("pred false (fieldname): %s != %s\n", q.FieldName, s.field)
			return false
		}
		for _, tag := range q.TagFilters {
			tagStr := string(tag)
			_, ok := s.tags[tagStr]
			if !ok {
				//fmt.Printf("pred false (tag): %s\n", tagStr)
				return false
			}

		}
		//fmt.Printf("pred true\n")
		return true
	}

	// filter all series against the predicate, deleting if not a match:
	ret := []series{}
	for s := range m {
		if pred(s) {
			ret = append(ret, *s)
		}
	}
	return ret
}

// A series maps to a time series in cassandra. All data in this type are just
// keys used in the database.
type series struct {
	table string // e.g. "series_bigint"
	id    string // e.g. "cpu,hostname=host_0,region=eu-central-1#usage_idle#2016-01-01"

	// parsed fields
	measurement string              // e.g. "cpu"
	tags        map[string]struct{} // e.g. {"hostname": "host_3"}
	field       string              // e.g. "usage_idle"
	timeBucket  timeBucket          // (UTC) e.g. "2016-01-01"
}

func newSeries(table, id string) series {
	s := series{
		table: table,
		id:    id,
	}

	s.parse()
	return s
}

func (s *series) parse() {
	// expected format:
	// cpu,hostname=host_0,region=eu-central-1,datacenter=eu-central-1a,rack=42,os=Ubuntu16.10,arch=x64,team=CHI,service=19,service_version=1,service_environment=staging#usage_idle#2016-01-01
	sections := strings.Split(s.id, "#")
	if len(sections) != 3 {
		fmt.Println(s.table)
		log.Fatal("logic error: invalid series id")
	}
	measurementAndTags := strings.Split(sections[0], ",")

	// parse measurement:
	s.measurement = measurementAndTags[0]

	// parse tags:
	tags := map[string]struct{}{}
	for _, tag := range measurementAndTags[1:] {
		if _, ok := tags[tag]; ok {
			log.Fatal("logic error: duplicate tag")
		}

		tags[tag] = struct{}{}
	}
	s.tags = tags

	// parse field name:
	s.field = sections[1]

	// parse time bucket:
	s.timeBucket = timeBucket(sections[2])
}

func fetchSeriesCollection(daemonUrl string) []series {
	cluster := gocql.NewCluster(daemonUrl)
	cluster.Keyspace = "measurements"
	cluster.Consistency = gocql.One
	cluster.ProtoVersion = 4
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	seriesCollection := []series{}

	for _, tableName := range blessedTables {
		var seriesId string
		iter := session.Query(fmt.Sprintf(`SELECT DISTINCT series_id FROM %s`, tableName)).Iter()
		for iter.Scan(&seriesId) {
			fmt.Println(tableName, seriesId)
			s := newSeries(tableName, seriesId)
			seriesCollection = append(seriesCollection, s)

		}
		if err := iter.Close(); err != nil {
			log.Fatal(err)
		}
	}
	return seriesCollection
}
