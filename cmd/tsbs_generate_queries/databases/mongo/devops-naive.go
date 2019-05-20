package mongo

import (
	"encoding/gob"
	"fmt"
	"time"

	"github.com/globalsign/mgo/bson"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/query"
)

func init() {
	// needed for serializing the mongo query to gob
	gob.Register([]interface{}{})
	gob.Register(map[string]interface{}{})
	gob.Register([]map[string]interface{}{})
	gob.Register(bson.M{})
	gob.Register([]bson.M{})
}

// NaiveDevops produces Mongo-specific queries for the devops use case.
type NaiveDevops struct {
	*devops.Core
}

// NewNaiveDevops makes an NaiveDevops object ready to generate Queries.
func NewNaiveDevops(start, end time.Time, scale int) *NaiveDevops {
	return &NaiveDevops{devops.NewCore(start, end, scale)}
}

// GenerateEmptyQuery returns an empty query.Mongo
func (d *NaiveDevops) GenerateEmptyQuery() query.Query {
	return query.NewMongo()
}

// GroupByTime selects the MAX for numMetrics metrics under 'cpu',
// per minute for nhosts hosts,
// e.g. in pseudo-SQL:
//
// SELECT minute, max(metric1), ..., max(metricN)
// FROM cpu
// WHERE (hostname = '$HOSTNAME_1' OR ... OR hostname = '$HOSTNAME_N')
// AND time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY minute ORDER BY minute ASC
func (d *NaiveDevops) GroupByTime(qi query.Query, nHosts, numMetrics int, timeRange time.Duration) {
	interval := d.Interval.MustRandWindow(timeRange)
	hostnames := d.GetRandomHosts(nHosts)
	metrics := devops.GetCPUMetricsSlice(numMetrics)

	bucketNano := time.Minute.Nanoseconds()
	pipelineQuery := []bson.M{
		{
			"$match": map[string]interface{}{
				"measurement": "cpu",
				"timestamp_ns": map[string]interface{}{
					"$gte": interval.StartUnixNano(),
					"$lt":  interval.EndUnixNano(),
				},
				"tags.hostname": map[string]interface{}{
					"$in": hostnames,
				},
			},
		},
		{
			"$project": map[string]interface{}{
				"_id": 0,
				"time_bucket": map[string]interface{}{
					"$subtract": []interface{}{
						"$timestamp_ns",
						map[string]interface{}{"$mod": []interface{}{"$timestamp_ns", bucketNano}},
					},
				},

				"fields": 1,
			},
		},
	}

	group := bson.M{
		"$group": bson.M{
			"_id": "$time_bucket",
		},
	}
	resultMap := group["$group"].(bson.M)
	for _, metric := range metrics {
		resultMap["max_"+metric] = bson.M{"$max": "$fields." + metric}
	}
	pipelineQuery = append(pipelineQuery, group)
	pipelineQuery = append(pipelineQuery, bson.M{"$sort": bson.M{"_id": 1}})

	humanLabel := []byte(fmt.Sprintf("Mongo [NAIVE] %d cpu metric(s), random %4d hosts, random %s by 1m", numMetrics, nHosts, timeRange))
	q := qi.(*query.Mongo)
	q.HumanLabel = humanLabel
	q.BsonDoc = pipelineQuery
	q.CollectionName = []byte("point_data")
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s (%s)", humanLabel, interval.StartString(), q.CollectionName))
}

// GroupByTimeAndPrimaryTag selects the AVG of numMetrics metrics under 'cpu' per device per hour for a day,
// e.g. in pseudo-SQL:
//
// SELECT AVG(metric1), ..., AVG(metricN)
// FROM cpu
// WHERE time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY hour, hostname ORDER BY hour, hostname
func (d *NaiveDevops) GroupByTimeAndPrimaryTag(qi query.Query, numMetrics int) {
	interval := d.Interval.MustRandWindow(devops.DoubleGroupByDuration)
	metrics := devops.GetCPUMetricsSlice(numMetrics)
	bucketNano := time.Hour.Nanoseconds()

	pipelineQuery := []bson.M{
		{
			"$match": bson.M{
				"measurement": "cpu",
				"timestamp_ns": bson.M{
					"$gte": interval.StartUnixNano(),
					"$lt":  interval.EndUnixNano(),
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

				"fields":      1,
				"measurement": 1,
				"tags":        "$tags.hostname",
			},
		},
	}

	// Add groupby operator
	group := bson.M{
		"$group": bson.M{
			"_id": bson.M{
				"time":     "$time_bucket",
				"hostname": "$tags",
			},
		},
	}
	resultMap := group["$group"].(bson.M)
	for _, metric := range metrics {
		resultMap["avg_"+metric] = bson.M{"$avg": "$fields." + metric}
	}
	pipelineQuery = append(pipelineQuery, group)

	// Add sort operator
	pipelineQuery = append(pipelineQuery, []bson.M{
		{"$sort": bson.M{"_id.hostname": 1}},
		{"$sort": bson.M{"_id.time": 1}},
	}...)
	pipelineQuery = append(pipelineQuery, bson.M{"$sort": bson.M{"_id.time": 1, "_id.hostname": 1}})

	humanLabel := devops.GetDoubleGroupByLabel("Mongo [NAIVE]", numMetrics)
	q := qi.(*query.Mongo)
	q.HumanLabel = []byte(humanLabel)
	q.BsonDoc = pipelineQuery
	q.CollectionName = []byte("point_data")
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s (%s)", humanLabel, interval.StartString(), q.CollectionName))
}
