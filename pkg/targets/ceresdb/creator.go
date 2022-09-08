package ceresdb

import (
	"context"
	"fmt"
	"strings"

	"github.com/jiacai2050/ceresdb_client_go/ceresdb"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets"
)

const timestampName = "timestamp"

var moreTagKeys map[string][]string

func init() {
	moreTagKeys = make(map[string][]string)
	moreTagKeys["diskio"] = []string{
		"serial",
	}
	moreTagKeys["disk"] = []string{
		"path",
		"fstype",
	}
	moreTagKeys["redis"] = []string{
		"port",
		"server",
	}
	moreTagKeys["nginx"] = []string{"server", "port"}
	moreTagKeys["net"] = []string{"interface"}

}

type dbCreator struct {
	ds      targets.DataSource
	headers *common.GeneratedDataHeaders
	config  *SpecificConfig
}

// loader.DBCreator interface implementation
func (d *dbCreator) Init() {
	d.headers = d.ds.Headers()
}

// loader.DBCreator interface implementation
func (d *dbCreator) DBExists(dbName string) bool { return true }

// loader.DBCreator interface implementation
func (d *dbCreator) CreateDB(dbName string) error {
	client, err := ceresdb.NewClient(d.config.CeresdbAddr)
	if err != nil {
		return err
	}
	for tableName, fieldColumns := range d.headers.FieldKeys {
		if err := d.createTable(client, tableName, fieldColumns); err != nil {
			return err
		}
	}
	return nil
}

func (d *dbCreator) createTable(client *ceresdb.Client, tableName string,
	fieldColumns []string) error {
	tagTypes, tagKeys := d.headers.TagTypes, d.headers.TagKeys
	columnDefs := make([]string, 0, len(fieldColumns)+len(tagTypes)+1) // one more timestamp column
	columnDefs = append(columnDefs, fmt.Sprintf("`%s` timestamp not null timestamp key", timestampName))
	for i, tagType := range tagTypes {
		columnDefs = append(columnDefs, fmt.Sprintf("`%s` %s tag", tagKeys[i], tagType))
	}
	if tagKeys, ok := moreTagKeys[tableName]; ok {
		for _, tagKey := range tagKeys {
			columnDefs = append(columnDefs, fmt.Sprintf("`%s` string tag", tagKey))
		}
	}
	for _, field := range fieldColumns {
		columnDefs = append(columnDefs, fmt.Sprintf("`%s` double", field))
	}

	tmpl := `
create table if not exists %s (
%s
) with (
enable_ttl = 'false',
storage_format = '%s'
);

`
	sql := fmt.Sprintf(tmpl, tableName, strings.Join(columnDefs, ","), d.config.StorageFormat)
	// fmt.Printf("sql = %s\n", sql)
	_, err := client.Query(context.TODO(), sql)
	return err
}

// loader.DBCreator interface implementation
func (d *dbCreator) RemoveOldDB(dbName string) error { return nil }
