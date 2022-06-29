package main

import (
	"fmt"
	"github.com/timescale/tsbs/pkg/query"
)

func debug_print_redistimeseries_reply(reply [][]interface{}, idx int, tq *query.RedisTimeSeries) {
	fmt.Println(fmt.Sprintf("Command reply. Total series %d", len(reply[idx])))
	for _, serie := range reply[idx] {
		converted_serie := serie.([]interface{})
		serie_name := string(converted_serie[0].([]uint8))
		fmt.Println(fmt.Sprintf("\tSerie name: %s", serie_name))
		serie_labels := converted_serie[1].([]interface{})
		fmt.Println(fmt.Sprintf("\tSerie labels:"))
		for _, kvpair := range serie_labels {
			kvpairc := kvpair.([]interface{})
			k := string(kvpairc[0].([]uint8))
			v := string(kvpairc[1].([]uint8))
			fmt.Println(fmt.Sprintf("\t\t%s: %s", k, v))
		}
		fmt.Println(fmt.Sprintf("\tSerie datapoints:"))
		serie_datapoints := converted_serie[2].([]interface{})
		if string(tq.CommandNames[idx]) == "TS.MGET" {
			ts := serie_datapoints[0].(int64)
			v := serie_datapoints[1].(string)
			fmt.Println(fmt.Sprintf("\t\tts: %d value: %s", ts, v))

		} else {
			for _, datapointpair := range serie_datapoints {
				datapoint := datapointpair.([]interface{})
				ts := datapoint[0].(int64)
				v := datapoint[1].(string)
				fmt.Println(fmt.Sprintf("\t\tts: %d value: %s", ts, v))
			}
		}
	}
}

func ByteArrayToInterfaceArray(qry [][]byte) []interface{} {
	commandArgs := make([]interface{}, len(qry))
	for i := 0; i < len(qry); i++ {
		commandArgs[i] = qry[i]
	}
	return commandArgs
}

func ByteArrayToStringArray(qry [][]byte) []string {
	commandArgs := make([]string, len(qry))
	for i := 0; i < len(qry); i++ {
		commandArgs[i] = string(qry[i])
	}
	return commandArgs
}
