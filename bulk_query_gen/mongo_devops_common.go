package main

import (
	"encoding/gob"
	"fmt"
	"math/rand"
	"time"

	"gopkg.in/mgo.v2/bson"
)

func init() {
	// needed for serializing the mongo query to gob
	gob.Register([]interface{}{})
	gob.Register(bson.DocElem{})
	gob.Register(bson.D{})
	gob.Register(bson.M{})
	gob.Register([]bson.D{})
	gob.Register([]bson.M{})
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

	hostnameClauses := []bson.M{}
	for _, h := range hostnames {
		hostnameClauses = append(hostnameClauses, bson.M{"key": "hostname", "val": h})
	}

	//combinedHostnameClause := fmt.Sprintf("[ %s ]", strings.Join(hostnameClauses, ", "))

	var bucketNano int64 = 1e9
	pipelineQuery := []bson.M{
		{
			"$match": bson.M{
				"measurement": "cpu",
				"timestamp_ns": bson.M{
					"$gte": interval.StartUnixNano(),
					"$lt":  interval.EndUnixNano(),
					//"$gte": 1451607326000000000,
					//"$lt": 1451610926000000000,
				},
				"field": "usage_user",
				"tags": bson.M{
					"$in": hostnameClauses,
					//bson.M[
					//bson.M{"key": "hostname", "val": "host_0"},
					//],
				},
			},
		},
		{
			"$project": bson.M{
				"_id": 0,
				"time_bucket": bson.M{
					"$subtract": []interface{}{
						"$timestamp_ns",
						bson.M{"$mod": []interface{}{"$timestamp_ns", bucketNano}},
					},
				},

				//"_ignored_tags": {
				//  $filter: {
				//     input: "$tags",
				//     as: "tag",
				//     cond: { $or: [
				//            { $eq: ["$$tag.key", "hostname" ]},
				//     ] },
				//  },
				//},
				"field":       1,
				"value":       1,
				"measurement": 1,
			},
		},
		{
			"$group": bson.M{
				"_id":       bson.M{"time_bucket": "$time_bucket", "tags": "$tags"},
				"max_value": bson.M{"$max": "$value"},
			},
		},
		{
			"$sort": bson.M{"_id.time_bucket": 1},
		},
	}

	humanLabel := []byte(fmt.Sprintf("Mongo max cpu, rand %4d hosts, rand 1hr by 1m", nhosts))
	q := qi.(*MongoQuery)
	q.HumanLabel = humanLabel
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	q.BsonDoc = pipelineQuery
	q.DatabaseName = []byte(dbName)
	q.CollectionName = []byte("points_data")
	q.FieldName = []byte("usage_user")
	q.MeasurementName = []byte("cpu")
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
