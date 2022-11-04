package main

import (
	"fmt"

	"github.com/apache/iotdb-client-go/client"
)

// DBCreator is an interface for a benchmark to do the initial setup of a database
// in preparation for running a benchmark against it.

type dbCreator struct {
	session client.Session
}

func (d *dbCreator) Init() {
	d.session = client.NewSession(&clientConfig)
	if err := d.session.Open(false, timeoutInMs); err != nil {
		errMsg := fmt.Sprintf("dbCreator init error, session is not open: %v\n", err)
		errMsg = errMsg + fmt.Sprintf("timeout setting: %d ms", timeoutInMs)
		fatal(errMsg)
	}
}

// get all Storage Group
func (d *dbCreator) getAllStorageGroup() ([]string, error) {
	var sql = "show storage group"
	sessionDataSet, err := d.session.ExecuteStatement(sql)
	if err != nil {
		return []string{}, err
	}

	var sgList []string

	for next, err := sessionDataSet.Next(); err == nil && next; next, err = sessionDataSet.Next() {
		for i := 0; i < sessionDataSet.GetColumnCount(); i++ {
			columnName := sessionDataSet.GetColumnName(i)
			switch sessionDataSet.GetColumnDataType(i) {
			case client.TEXT:
				sgList = append(sgList, sessionDataSet.GetText(columnName))
			default:
			}
		}
	}
	return sgList, nil
}

func (d *dbCreator) DBExists(dbName string) bool {
	// d.session = client.NewSession(&clientConfig)
	// defer d.session.Close()

	sgList, err := d.getAllStorageGroup()
	if err != nil {
		fatal("DBExists error: %v", err)
		return false
	}
	sg := fmt.Sprintf("root.%s", dbName)
	for _, thisSG := range sgList {
		if thisSG == sg {
			return true
		}
	}
	return false
}

func (d *dbCreator) CreateDB(dbName string) error {
	return nil
}

func (d *dbCreator) RemoveOldDB(dbName string) error {
	// d.session = client.NewSession(&clientConfig)
	// defer d.session.Close()

	sg := fmt.Sprintf("root.%s", dbName)
	_, err := d.session.DeleteStorageGroup(sg)
	return err
}
