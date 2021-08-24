package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type dbCreator struct {
	questdbRESTEndPoint string
}

func (d *dbCreator) Init() {
	d.questdbRESTEndPoint = questdbRESTEndPoint
}

func (d *dbCreator) DBExists(dbName string) bool {
	r, err := execQuery(questdbRESTEndPoint, "SHOW TABLES")
	if err != nil {
		panic(fmt.Errorf("fatal error, failed to query questdb: %s", err))
	}
	for i, v := range r.Dataset {
		if i >= 0 && v[0] == "cpu" {
			panic(fmt.Errorf("fatal error, cpu table already exists"))
		}
	}
	// Create minimal table with o3 params
	//        r, err = execQuery(questdbRESTEndPoint, "CREATE TABLE cpu (hostname SYMBOL, region SYMBOL, datacenter SYMBOL, rack SYMBOL, os SYMBOL, arch SYMBOL, team SYMBOL, service SYMBOL, service_version SYMBOL, service_environment SYMBOL, usage_user LONG, usage_system LONG, usage_idle LONG, usage_nice LONG, usage_iowait LONG, usage_irq LONG, usage_softirq LONG, usage_steal LONG, usage_guest LONG, usage_guest_nice LONG, timestamp TIMESTAMP) timestamp(timestamp) PARTITION BY DAY WITH o3MaxUncommittedRows=500000, o3CommitHysteresis=300s")
	//        if err != nil {
	//          panic(fmt.Errorf("fatal error, failed to create cpu table: %s", err))
	//        }

	return false
}

func (d *dbCreator) RemoveOldDB(dbName string) error {
	return nil
}

func (d *dbCreator) CreateDB(dbName string) error {
	time.Sleep(time.Second)
	return nil
}

type QueryResponseColumns struct {
	Name string
	Type string
}

type QueryResponse struct {
	Query   string
	Columns []QueryResponseColumns
	Dataset [][]interface{}
	Count   int
	Error   string
}

func execQuery(uriRoot string, query string) (QueryResponse, error) {
	var qr QueryResponse
	if strings.HasSuffix(uriRoot, "/") {
		uriRoot = uriRoot[:len(uriRoot)-1]
	}
	uriRoot = uriRoot + "/exec?query=" + url.QueryEscape(query)
	resp, err := http.Get(uriRoot)
	if err != nil {
		return qr, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return qr, err
	}
	err = json.Unmarshal(body, &qr)
	if err != nil {
		return qr, err
	}
	if qr.Error != "" {
		return qr, errors.New(qr.Error)
	}
	return qr, nil
}
