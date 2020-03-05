package akumuli

import (
	"bufio"
)

// loader.DBCreator interface implementation
type dbCreator struct {
}

// loader.DBCreator interface implementation
func (d *dbCreator) Init() {
}

// loader.DBCreator interface implementation
func (d *dbCreator) readDataHeader(br *bufio.Reader) {
}

// loader.DBCreator interface implementation
func (d *dbCreator) DBExists(dbName string) bool {
	return false
}

// loader.DBCreator interface implementation
func (d *dbCreator) RemoveOldDB(dbName string) error {
	return nil
}

// loader.DBCreator interface implementation
func (d *dbCreator) CreateDB(dbName string) error {
	return nil
}
