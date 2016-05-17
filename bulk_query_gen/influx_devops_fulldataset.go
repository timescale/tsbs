package main

//import "time"
//
//// InfluxDevopsFullDataset produces Influx-specific queries for the devops full-dataset case.
//type InfluxDevopsFullDataset struct {
//	InfluxDevops
//}
//
//func NewInfluxDevopsFullDataset(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
//	underlying := newInfluxDevopsCommon(dbConfig, start, end).(*InfluxDevops)
//	return &InfluxDevopsFullDataset{
//		InfluxDevops: *underlying,
//	}
//
//}
//
//func (d *InfluxDevopsFullDataset) Dispatch(i int, q *Query, scaleVar int) {
//	d.MaxCPUUsageHourByMinuteOneHost(q, scaleVar)
//}
