package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/mailru/go-clickhouse" //_ "github.com/kshvakov/clickhouse"
	"github.com/timescale/tsbs/load"
)

type syncCSI struct {
	// Map hostname to tags.id for this host
	m     map[string]int64
	mutex *sync.RWMutex
}

func newSyncCSI() *syncCSI {
	return &syncCSI{
		m:     make(map[string]int64),
		mutex: &sync.RWMutex{},
	}
}

// globalSyncCSI is used when data is not hashed by some function to a worker consistently so
// therefore all workers need to know about the same map from hostname -> tags_id
var globalSyncCSI = newSyncCSI()

// Process part of incoming data - insert into tables
func (p *processor) processCSI(tableName string, rows []*insertData) (uint64, float64) {
	return p.processColumnModel(tableName, rows)

}

func (p *processor) processColumnModel(tableName string, rows []*insertData) (uint64, float64) {
	tagRows := make([][]string, 0, len(rows))
	dataRows := make([][]interface{}, 0, len(rows))
	commonTagsLen := len(tagCols["cpu_tags_metrics"])
	colLen := len(tableCols["cpu_tags_metrics"]) + 1 // +1 to add time col
	ret := uint64(0)

	for _, data := range rows {
		// Split the tags into individual common tags and
		// an extra bit leftover for non-common tags that need to be added separately.
		// For each of the common tags, remove everything after = in the form <label>=<val>
		// since we won't need it.
		// tags line ex.:
		// hostname=host_0,region=eu-west-1,datacenter=eu-west-1b,rack=67,os=Ubuntu16.10,arch=x86,team=NYC,service=7,service_version=0,service_environment=production
		tags := strings.SplitN(data.tags, ",", commonTagsLen+1)
		// tags = (
		//	hostname=host_0
		//	region=eu-west-1
		//	datacenter=eu-west-1b
		// )
		// extract value of each tag
		// tags = (
		//	host_0
		//	eu-west-1
		//	eu-west-1b
		// )
		for i := 0; i < commonTagsLen; i++ {
			tags[i] = strings.Split(tags[i], "=")[1]
		}

		// fields line ex.:
		// 1451606400000000000,58,2,24,61,22,63,6,44,80,38
		metrics := strings.Split(data.fields, ",")

		// Count number of metrics processed
		ret += uint64(len(metrics) - 1) // 1-st field is timestamp, do not count it
		// metrics = (
		// 	1451606400000000000,
		// 	58,
		// )

		// Build string TimeStamp as '2006-01-02 15:04:05.999999 -0700'
		// convert time from 1451606400000000000 (int64 UNIX TIMESTAMP with nanoseconds)
		timestampNano, err := strconv.ParseInt(metrics[0], 10, 64)
		if err != nil {
			panic(err)
		}
		timeUTC := time.Unix(0, timestampNano)

		tagRows = append(tagRows, tags)
		var variadicArgs []interface{} = make([]interface{}, len(tags))
		// And all the rest of column values afterwards
		for i, value := range tags {
			variadicArgs[i] = convertBasedOnType(tagColumnTypes[i], value)
		}

		// the column between tag columns and metric columns in table is time
		variadicArgs = append(variadicArgs, timeUTC)
		for _, v := range metrics[1:] {
			if v == "" {
				variadicArgs = append(variadicArgs, nil)
				continue
			}
			f64, err := strconv.ParseFloat(v, 64)
			if err != nil {
				panic(err)
			}
			variadicArgs = append(variadicArgs, f64)
		}

		dataRows = append(dataRows, variadicArgs)
	}

	// Prepare column names
	cols := make([]string, 0, colLen)
	cols = append(cols, tagCols["cpu_tags_metrics"]...)
	cols = append(cols, "time")
	cols = append(cols, metricCols["cpu_tags_metrics"]...)

	tx := p.db.MustBegin()
	// INSERT statement template
	sql := fmt.Sprintf("INSERT INTO %s.cpu_tags_metrics (%s) VALUES (%s)",
		loader.DBName,
		strings.Join(cols, ","),
		strings.Repeat(",?", len(cols))[1:]) // We need '?,?,?', but repeat ",?" thus we need to chop off 1-st char

	if debug > 0 {
		fmt.Println(sql)
	}

	// calc the sql execute time
	start := time.Now()

	stmt, err := tx.Prepare(sql)
	defer stmt.Close()
	for _, r := range dataRows {
		_, err := stmt.Exec(r...)
		if err != nil {
			panic(err)
		}
	}

	if err = tx.Commit(); err != nil {
		panic(err)
	}

	end := time.Now()

	return ret, end.Sub(start).Seconds()
}

// load.Processor interface implementation
type processor struct {
	db  *sqlx.DB
	csi *syncCSI
}

// load.Processor interface implementation
func (p *processor) Init(workerNum int, doLoad bool) {
	if doLoad {
		p.db = sqlx.MustConnect(dbType, getConnectString(true))
		if hashWorkers {
			p.csi = newSyncCSI()
		} else {
			p.csi = globalSyncCSI
		}
	}
}

// load.ProcessorCloser interface implementation
func (p *processor) Close(doLoad bool) {
	if doLoad {
		p.db.Close()
	}
}

// load.Processor interface implementation
func (p *processor) ProcessBatch(b load.Batch, doLoad bool) (uint64, uint64) {
	batches := b.(*tableArr)
	rowCnt := 0
	metricCnt := uint64(0)
	oneWorkInsertTookSum := float64(0)
	for tableName, rows := range batches.m {
		rowCnt += len(rows)
		if doLoad {
			metricNum, oneWorkInsertTook := p.processCSI(tableName, rows)
			metricCnt += metricNum
			oneWorkInsertTookSum += oneWorkInsertTook
			if logBatches {
				batchSize := len(rows)
				fmt.Printf("BATCH: batchsize %d row rate %f/sec (A BATCH Took %v s)\n", batchSize, float64(batchSize)/oneWorkInsertTook, oneWorkInsertTook)
			}
		}
	}
	batches.m = map[string][]*insertData{}
	batches.cnt = 0

	return metricCnt, uint64(rowCnt)
}

func convertBasedOnType(serializedType, value string) interface{} {
	if value == "" {
		return nil
	}

	switch serializedType {
	case "string":
		return value
	case "float32":
		f, err := strconv.ParseFloat(value, 32)
		if err != nil {
			panic(fmt.Sprintf("could not parse '%s' to float32", value))
		}
		return float32(f)
	case "float64":
		f, err := strconv.ParseFloat(value, 64)
		if err != nil {
			panic(fmt.Sprintf("could not parse '%s' to float64", value))
		}
		return f
	case "int64":
		i, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			panic(fmt.Sprintf("could not parse '%s' to int64", value))
		}
		return i
	case "int32":
		i, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			panic(fmt.Sprintf("could not parse '%s' to int64", value))
		}
		return int32(i)
	default:
		panic(fmt.Sprintf("unrecognized type %s", serializedType))
	}
}
