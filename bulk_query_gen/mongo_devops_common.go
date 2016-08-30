package main

import (
	"encoding/gob"
	"fmt"
	"math/rand"
	"time"
)

type S []interface{}
type M map[string]interface{}

func init() {
	// needed for serializing the mongo query to gob
	gob.Register(S{})
	gob.Register(M{})
	gob.Register([]M{})
}

// MongoDevops produces Mongo-specific queries for the devops use case.
type MongoDevops struct {
	AllInterval TimeInterval
}

// NewMongoDevops makes an MongoDevops object ready to generate Queries.
func NewMongoDevops(_ DatabaseConfig, start, end time.Time) QueryGenerator {
	if !start.Before(end) {
		panic("bad time order")
	}
	return &MongoDevops{
		AllInterval: NewTimeInterval(start, end),
	}
}

// Dispatch fulfills the QueryGenerator interface.
func (d *MongoDevops) Dispatch(i, scaleVar int) Query {
	q := NewMongoQuery() // from pool
	devopsDispatchAll(d, i, q, scaleVar)
	return q
}

// MaxCPUUsageHourByMinuteOneHost populates a Query for getting the maximum CPU
// usage for one host over the course of an hour.
func (d *MongoDevops) MaxCPUUsageHourByMinuteOneHost(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*MongoQuery), scaleVar, 1)
}

// MaxCPUUsageHourByMinuteTwoHosts populates a Query for getting the maximum CPU
// usage for two hosts over the course of an hour.
func (d *MongoDevops) MaxCPUUsageHourByMinuteTwoHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*MongoQuery), scaleVar, 2)
}

// MaxCPUUsageHourByMinuteFourHosts populates a Query for getting the maximum CPU
// usage for four hosts over the course of an hour.
func (d *MongoDevops) MaxCPUUsageHourByMinuteFourHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*MongoQuery), scaleVar, 4)
}

// MaxCPUUsageHourByMinuteEightHosts populates a Query for getting the maximum CPU
// usage for four hosts over the course of an hour.
func (d *MongoDevops) MaxCPUUsageHourByMinuteEightHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*MongoQuery), scaleVar, 8)
}

// MaxCPUUsageHourByMinuteSixteenHosts populates a Query for getting the maximum CPU
// usage for four hosts over the course of an hour.
func (d *MongoDevops) MaxCPUUsageHourByMinuteSixteenHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*MongoQuery), scaleVar, 16)
}

func (d *MongoDevops) MaxCPUUsageHourByMinuteThirtyTwoHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*MongoQuery), scaleVar, 32)
}

func (d *MongoDevops) maxCPUUsageHourByMinuteNHosts(qi Query, scaleVar, nhosts int) {
	interval := d.AllInterval.RandWindow(time.Hour)
	nn := rand.Perm(scaleVar)[:nhosts]

	hostnames := []string{}
	for _, n := range nn {
		hostnames = append(hostnames, fmt.Sprintf("host_%d", n))
	}

	hostnameClauses := []M{}
	for _, h := range hostnames {
		hostnameClauses = append(hostnameClauses, M{"key": "hostname", "val": h})
	}

	var bucketNano int64 = time.Minute.Nanoseconds()
	pipelineQuery := []M{
		{
			"$match": M{
				"measurement": "cpu",
				"timestamp_ns": M{
					"$gte": interval.StartUnixNano(),
					"$lt":  interval.EndUnixNano(),
				},
				"field": "usage_user",
				"tags": M{
					"$in": hostnameClauses,
				},
			},
		},
		{
			"$project": M{
				"_id": 0,
				"time_bucket": M{
					"$subtract": S{
						"$timestamp_ns",
						M{"$mod": S{"$timestamp_ns", bucketNano}},
					},
				},

				"field":       1,
				"value":       1,
				"measurement": 1,
			},
		},
		{
			"$group": M{
				"_id":       M{"time_bucket": "$time_bucket", "tags": "$tags"},
				"max_value": M{"$max": "$value"},
			},
		},
		{
			"$sort": M{"_id.time_bucket": 1},
		},
	}

	humanLabel := []byte(fmt.Sprintf("Mongo max cpu, rand %4d hosts, rand 1hr by 1m", nhosts))
	q := qi.(*MongoQuery)
	q.HumanLabel = humanLabel
	q.BsonDoc = pipelineQuery
	q.DatabaseName = []byte("benchmark_db")
	q.CollectionName = []byte("point_data")
	q.MeasurementName = []byte("cpu")
	q.FieldName = []byte("usage_user")
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s (%s, %s, %s, %s)", humanLabel, interval.StartString(), q.DatabaseName, q.CollectionName, q.MeasurementName, q.FieldName))
	q.TimeStart = interval.Start
	q.TimeEnd = interval.End
	q.GroupByDuration = time.Minute
}

func (d *MongoDevops) MeanCPUUsageDayByHourAllHostsGroupbyHost(qi Query, scaleVar int) {
	//	if scaleVar > 10000 {
	//		// TODO: does this apply to mongo?
	//		panic("scaleVar > 10000 implies size > 10000, which is not supported on elasticsearch. see https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-from-size.html")
	//	}
	//
	//	interval := d.AllInterval.RandWindow(24 * time.Hour)
	//
	//	body := new(bytes.Buffer)
	//	mustExecuteTemplate(mongoFleetGroupByHostnameQuery, body, MongoFleetQueryParams{
	//		Start:         interval.StartString(),
	//		End:           interval.EndString(),
	//		Bucket:        "1h",
	//		Field:         "usage_user",
	//		HostnameCount: scaleVar,
	//	})
	//
	//	humanLabel := []byte("Mongo mean cpu, all hosts, rand 1day by 1hour")
	//	q := qi.(*HTTPQuery)
	//	q.HumanLabel = humanLabel
	//	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	//	q.Method = []byte("POST")
	//
	//	q.Path = []byte("/cpu/_search")
	//	q.Body = body.Bytes()
}
