package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	siridb "github.com/SiriDB/go-siridb-connector"
)

const (
	account       = "sa"
	password      = "siri"
	timePrecision = "ns"
	bufferSize    = 1024
	durationNum   = "1w"
	durationLog   = "1d"
)

type dbCreator struct {
	connection []*siridb.Connection
	hosts      []string
	replica    []string
}

// Init should set up any connection or other setup for talking to the DB, but should NOT create any databases
func (d *dbCreator) Init() {
	d.hosts = strings.Split(hosts, ",")
	d.connection = make([]*siridb.Connection, 0)
	for _, hostport := range d.hosts {
		x := strings.Split(hostport, ":")
		host := x[0]
		port, err := strconv.ParseUint(x[1], 10, 16)
		if err != nil {
			fatal(err)
		}
		d.connection = append(d.connection, siridb.NewConnection(host, uint16(port)))
	}
}

// DBExists checks if a database with the given name currently exists.
func (d *dbCreator) DBExists(dbName string) bool {
	for _, conn := range d.connection {
		if err := conn.Connect(dbUser, dbPass, dbName); err == nil {
			return true
		}
	}
	return false
}

func (d *dbCreator) RemoveOldDB(dbName string) error {
	msg := errors.New("database cannot be dropped, you need to stop the server and remove the database directory in your DBPATH")
	return msg
}

// CreateDB creates a database with the given name.
func (d *dbCreator) CreateDB(dbName string) error {
	defer d.Close()
	optionsNewDB := make(map[string]interface{})
	optionsNewDB["dbname"] = dbName
	optionsNewDB["time_precision"] = timePrecision
	optionsNewDB["buffer_size"] = bufferSize
	optionsNewDB["duration_num"] = durationNum
	optionsNewDB["duration_log"] = durationLog

	if _, err := d.connection[0].Manage(account, password, siridb.AdminNewDatabase, optionsNewDB); err != nil {
		return err
	}

	if len(d.hosts) == 2 {
		h := strings.Split(d.hosts[0], ":")
		host := h[0]
		port, err := strconv.ParseUint(h[1], 10, 16)
		if err != nil {
			fatal(err)
		}

		if !replica {
			optionsNewPool := make(map[string]interface{})
			optionsNewPool["dbname"] = dbName
			optionsNewPool["host"] = host
			optionsNewPool["port"] = port
			optionsNewPool["username"] = dbUser
			optionsNewPool["password"] = dbPass

			if _, err := d.connection[1].Manage(account, password, siridb.AdminNewPool, optionsNewPool); err != nil {
				return err
			}

		} else {
			optionsNewReplica := make(map[string]interface{})
			optionsNewReplica["dbname"] = dbName
			optionsNewReplica["host"] = host
			optionsNewReplica["port"] = port
			optionsNewReplica["username"] = dbUser
			optionsNewReplica["password"] = dbPass
			optionsNewReplica["pool"] = 0

			if _, err := d.connection[1].Manage(account, password, siridb.AdminNewReplica, optionsNewReplica); err != nil {
				return err
			}
		}
	} else if len(d.hosts) > 2 {
		fatal(fmt.Sprintf("You have provided %d hosts, but only 2 hosts are allowed", len(d.hosts)))
	}
	return nil
}

func (d *dbCreator) Close() {
	for _, conn := range d.connection {
		conn.Close()
	}
}
