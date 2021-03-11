package timestream

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/timestreamwrite"
	"github.com/pkg/errors"
	"github.com/timescale/tsbs/pkg/targets"
	"log"
	"time"
)

const (
	checkTablesMaxAttempts          = 10
	checkTablesSecondsBetweenChecks = 10
)

type dbCreator struct {
	writeSvc                           *timestreamwrite.TimestreamWrite
	ds                                 targets.DataSource
	memoryRetentionPeriodInHours       int64
	magneticStoreRetentionPeriodInDays int64
}

func (d *dbCreator) Init() {
	// read headers from data source so PostCreate can create the tables
	d.ds.Headers()
}

func (d *dbCreator) DBExists(dbName string) bool {
	describeDatabaseInput := &timestreamwrite.DescribeDatabaseInput{
		DatabaseName: &dbName,
	}
	_, err := d.writeSvc.DescribeDatabase(describeDatabaseInput)
	if err != nil {
		// Check if error was "database doesn't exist"
		_, ok := err.(*timestreamwrite.ResourceNotFoundException)
		if ok {
			return false
		}
		panic("could not execute 'describe database': " + err.Error())
	}

	return true
}

func (d *dbCreator) CreateDB(dbName string) error {
	log.Println("Creating database " + dbName)
	createDatabaseInput := &timestreamwrite.CreateDatabaseInput{
		DatabaseName: &dbName,
	}

	if _, err := d.writeSvc.CreateDatabase(createDatabaseInput); err != nil {
		return errors.Wrap(err, "could not create database "+dbName)
	}
	return nil
}

func (d *dbCreator) RemoveOldDB(dbName string) error {
	log.Println("Removing existing database " + dbName)
	listTables := &timestreamwrite.ListTablesInput{DatabaseName: &dbName}
	tablesOutput, err := d.writeSvc.ListTables(listTables)
	if err != nil {
		return errors.Wrap(err, "could not check existing tables in "+dbName)
	}
	for _, table := range tablesOutput.Tables {
		deleteTableInput := &timestreamwrite.DeleteTableInput{
			DatabaseName: &dbName,
			TableName:    table.TableName,
		}
		if _, err := d.writeSvc.DeleteTable(deleteTableInput); err != nil {
			return errors.Wrap(err, "could not delete table "+*table.TableName+" in db "+dbName)
		}
	}
	deleteDatabaseInput := &timestreamwrite.DeleteDatabaseInput{DatabaseName: &dbName}
	if _, err := d.writeSvc.DeleteDatabase(deleteDatabaseInput); err != nil {
		return errors.Wrap(err, "could not delete database "+dbName)
	}
	return nil
}

// Timestream doesn't need to create the complete schema, just the tables
func (d *dbCreator) PostCreateDB(dbName string) error {
	log.Println("Creating Timestream tables")
	headers := d.ds.Headers()
	var requiredTables []string
	for tableName := range headers.FieldKeys {
		requiredTables = append(requiredTables, tableName)
		createTableInput := &timestreamwrite.CreateTableInput{
			DatabaseName: &dbName,
			RetentionProperties: &timestreamwrite.RetentionProperties{
				MagneticStoreRetentionPeriodInDays: &d.magneticStoreRetentionPeriodInDays,
				MemoryStoreRetentionPeriodInHours:  &d.memoryRetentionPeriodInHours,
			},
			TableName: &tableName,
		}
		_, err := d.writeSvc.CreateTable(createTableInput)
		if _, ok := err.(*timestreamwrite.ConflictException); !ok {
			return errors.Wrap(err, "could not create table '"+tableName+"': ")
		} else {
			log.Println("Table " + tableName + " exists, skipping create")
		}
	}

	fmt.Println("DB created, checking table status")
	if err := d.waitForTables(dbName, requiredTables); err != nil {
		return errors.Wrap(err, "could not create timestream tables")
	}
	return nil
}

func (d *dbCreator) waitForTables(dbName string, requiredTables []string) error {
	numAttempts := 0
	for {
		tablesInDb, err := d.listTableStatus(dbName)
		if err != nil {
			return errors.Wrap(err, "could not check if all tables were created")
		}
		if allTablesActive, err := checkTableStatus(tablesInDb, requiredTables); err != nil {
			return err
		} else if allTablesActive {
			break
		}
		log.Printf("Not all tables are active, waiting %d seconds", checkTablesSecondsBetweenChecks)
		numAttempts++
		if numAttempts >= checkTablesMaxAttempts {
			return fmt.Errorf("tables not created and active in time")
		}
		time.Sleep(checkTablesSecondsBetweenChecks * time.Second)
	}
	return nil

}
func (d *dbCreator) listTableStatus(dbName string) (tableStatus map[string]string, err error) {
	listTables := &timestreamwrite.ListTablesInput{DatabaseName: &dbName}
	tablesOutput, err := d.writeSvc.ListTables(listTables)
	if err != nil {
		return nil, errors.Wrap(err, "could not check existing tables in "+dbName)
	}
	tableStatus = make(map[string]string, len(tablesOutput.Tables))
	for _, table := range tablesOutput.Tables {
		tableName := *table.TableName
		tableStatus[tableName] = *table.TableStatus
	}
	return tableStatus, nil
}

func checkTableStatus(tableStatus map[string]string, requiredTables []string) (bool, error) {
	for _, table := range requiredTables {
		status, ok := tableStatus[table]
		if !ok {
			return false, fmt.Errorf("required table '%s' not found in db", table)
		}
		if status != timestreamwrite.TableStatusActive {
			return false, nil
		}
	}
	return true, nil
}
