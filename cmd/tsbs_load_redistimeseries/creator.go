package main

import (
	redistimeseries "github.com/RedisTimeSeries/redistimeseries-go"
)

type dbCreator struct {
	client *redistimeseries.Client
}

func (d *dbCreator) Init() {
	d.client = redistimeseries.NewClient(host, "test_client", nil)
}

func (d *dbCreator) DBExists(dbName string) bool {
	return true
}

func (d *dbCreator) RemoveOldDB(dbName string) error {
	return nil
}

func (d *dbCreator) CreateDB(dbName string) error {
	return nil
}

func (d *dbCreator) Close() {
	conn := d.client.Pool.Get()
	conn.Close()
}
