package ceresdb

import "github.com/timescale/tsbs/pkg/data/usecases/common"

type dbCreator struct {
	headers *common.GeneratedDataHeaders
}

func (d *dbCreator) Init() {}

func (d *dbCreator) DBExists(dbName string) bool { return true }

func (d *dbCreator) CreateDB(dbName string) error {
	for tableName, fieldColumns := range d.headers.FieldKeys {
		// tableName: cpu
		// fieldColumns: usage_user...

		createTable(tableName, fieldColumns)
	}
	return nil
}

func createTable(tableName string, fieldColumns []string) {
	// return nil
}
func (d *dbCreator) RemoveOldDB(dbName string) error { return nil }
