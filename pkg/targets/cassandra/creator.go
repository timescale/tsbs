package cassandra

import (
	"fmt"
	"github.com/gocql/gocql"
	"log"
	"strings"
	"time"
)

// Map of user specified strings to gocql consistency settings
var consistencyMapping = map[string]gocql.Consistency{
	"ALL":    gocql.All,
	"ANY":    gocql.Any,
	"QUORUM": gocql.Quorum,
	"ONE":    gocql.One,
	"TWO":    gocql.Two,
	"THREE":  gocql.Three,
}

type dbCreator struct {
	globalSession     *gocql.Session
	clientSession     *gocql.Session
	consistencyLevel  string
	hosts             string
	replicationFactor int
	writeTimeout      time.Duration
}

func (d *dbCreator) Init() {
	cluster := gocql.NewCluster(strings.Split(d.hosts, ",")...)
	cluster.Consistency = consistencyMapping[d.consistencyLevel]
	cluster.ProtoVersion = 4
	cluster.Timeout = 10 * time.Second
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	d.globalSession = session
}

func (d *dbCreator) DBExists(dbName string) bool {
	iter := d.globalSession.Query(fmt.Sprintf("SELECT keyspace_name FROM system_schema.keyspaces;")).Iter()
	defer iter.Close()
	row := ""
	for iter.Scan(&row) {
		if row == dbName {
			return true
		}
	}
	return false
}

func (d *dbCreator) RemoveOldDB(dbName string) error {
	if err := d.globalSession.Query(fmt.Sprintf("drop keyspace if exists %s;", dbName)).Exec(); err != nil {
		return err
	}
	return nil
}

func (d *dbCreator) CreateDB(dbName string) error {
	defer d.globalSession.Close()
	replicationConfiguration := fmt.Sprintf("{ 'class': 'SimpleStrategy', 'replication_factor': %d }", d.replicationFactor)
	if err := d.globalSession.Query(fmt.Sprintf("create keyspace %s with replication = %s;", dbName, replicationConfiguration)).Exec(); err != nil {
		return err
	}
	for _, cassandraTypename := range []string{"bigint", "float", "double", "boolean", "blob"} {
		q := fmt.Sprintf(`CREATE TABLE %s.series_%s (
					series_id text,
					timestamp_ns bigint,
					value %s,
					PRIMARY KEY (series_id, timestamp_ns)
				 )
				 WITH COMPACT STORAGE;`,
			dbName, cassandraTypename, cassandraTypename)
		if err := d.globalSession.Query(q).Exec(); err != nil {
			return err
		}
	}
	return nil
}

func (d *dbCreator) PostCreateDB(dbName string) error {
	cluster := gocql.NewCluster(strings.Split(d.hosts, ",")...)
	cluster.Keyspace = dbName
	cluster.Timeout = d.writeTimeout
	cluster.Consistency = consistencyMapping[d.consistencyLevel]
	cluster.ProtoVersion = 4
	session, err := cluster.CreateSession()
	if err != nil {
		return err
	}
	d.clientSession = session
	return nil
}

func (d *dbCreator) Close() {
	d.clientSession.Close()
}
