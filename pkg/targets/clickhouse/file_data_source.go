package clickhouse

import (
	"bufio"
	"strings"

	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
)

// scan.PointDecoder interface implementation
type fileDataSource struct {
	scanner *bufio.Scanner
	//cached headers (should be read only at start of file)
	headers *common.GeneratedDataHeaders
}

// scan.PointDecoder interface implementation
func (d *fileDataSource) NextItem() data.LoadedPoint {
	// Data Point Example
	// tags,hostname=host_0,region=eu-west-1,datacenter=eu-west-1b,rack=67,os=Ubuntu16.10,arch=x86,team=NYC,service=7,service_version=0,service_environment=production
	// cpu,1451606400000000000,58,2,24,61,22,63,6,44,80,38

	newPoint := &insertData{}
	ok := d.scanner.Scan()
	if !ok && d.scanner.Err() == nil {
		// nothing scanned & no error = EOF
		return data.LoadedPoint{}
	} else if !ok {
		fatal("scan error: %v", d.scanner.Err())
		return data.LoadedPoint{}
	}

	// The first line is a CSV line of tags with the first element being "tags"
	// Ex.:
	// tags,hostname=host_0,region=eu-west-1,datacenter=eu-west-1b,rack=67,os=Ubuntu16.10,arch=x86,team=NYC,service=7,service_version=0,service_environment=production
	parts := strings.SplitN(d.scanner.Text(), ",", 2) // prefix & then rest of line
	prefix := parts[0]
	if prefix != tagsPrefix {
		fatal("data file in invalid format; got %s expected %s", prefix, tagsPrefix)
		return data.LoadedPoint{}
	}
	newPoint.tags = parts[1]

	// Scan again to get the data line
	// cpu,1451606400000000000,58,2,24,61,22,63,6,44,80,38
	ok = d.scanner.Scan()
	if !ok {
		fatal("scan error: %v", d.scanner.Err())
		return data.LoadedPoint{}
	}
	parts = strings.SplitN(d.scanner.Text(), ",", 2) // prefix & then rest of line
	prefix = parts[0]
	newPoint.fields = parts[1]

	return data.NewLoadedPoint(&point{
		table: prefix,
		row:   newPoint,
	})
}

func (d *fileDataSource) Headers() *common.GeneratedDataHeaders {
	if d.headers != nil {
		return d.headers
	}
	// First N lines are header, describing data structure.
	// The first line containing tags table name ('tags') followed by list of tags, comma-separated.
	// Ex.: tags,hostname,region,datacenter,rack,os,arch,team,service,service_version
	// The second through N-1 line containing table name (ex.: 'cpu') followed by list of column names,
	// comma-separated. Ex.: cpu,usage_user,usage_system,usage_idle,usage_nice,usage_iowait,usage_irq,usage_softirq
	// The last line being blank to separate from the data
	//
	// Header example:
	// tags,hostname,region,datacenter,rack,os,arch,team,service,service_version,service_environment
	// cpu,usage_user,usage_system,usage_idle,usage_nice,usage_iowait,usage_irq,usage_softirq,usage_steal,usage_guest,usage_guest_nice
	// disk,total,free,used,used_percent,inodes_total,inodes_free,inodes_used
	// nginx,accepts,active,handled,reading,requests,waiting,writing
	var tags string
	var cols []string
	i := 0
	for {
		var line string
		ok := d.scanner.Scan()
		if !ok && d.scanner.Err() == nil { // nothing scanned & no error = EOF
			fatal("reached EOF, but not enough things scanned")
			return nil
		} else if !ok {
			fatal("scan error: %v", d.scanner.Err())
			return nil
		}
		if i == 0 {
			// read first line - list of tags
			tags = d.scanner.Text()
			tags = strings.TrimSpace(tags)
		} else {
			// read the second and further lines - metrics descriptions
			line = d.scanner.Text()
			line = strings.TrimSpace(line)
			if len(line) == 0 {
				// empty line - end of header
				break
			}
			// append new table/columns set to the list of tables/columns set
			cols = append(cols, line)
		}
		i++
	}

	// tags content:
	//tags,hostname,region,datacenter,rack,os,arch,team,service,service_version,service_environment
	//
	// Parts would contain
	// 0: tags - reserved word - tags mark
	// 1:
	// N: actual tags
	// so we'll use tags[1:] for tags specification
	parts := strings.Split(tags, ",")
	if parts[0] != "tags" {
		fatal("input header in wrong format. got '%s', expected 'tags'", parts[0])
		return nil
	}
	tagNames, tagTypes := extractTagNamesAndTypes(parts[1:])
	fieldKeys := make(map[string][]string)
	// cols content are lines (metrics descriptions) as:
	// cpu,usage_user,usage_system,usage_idle,usage_nice,usage_iowait,usage_irq,usage_softirq,usage_steal,usage_guest,usage_guest_nice
	// disk,total,free,used,used_percent,inodes_total,inodes_free,inodes_used
	// nginx,accepts,active,handled,reading,requests,waiting,writing
	// generalised description:
	// tableName,fieldName1,...,fieldNameX
	for _, colsForMeasure := range cols {
		tableSpec := strings.Split(colsForMeasure, ",")
		// tableSpec contain
		// 0: table name
		// 1: table column name 1
		// N: table column name N

		// Ex.: cpu OR disk OR nginx
		tableName := tableSpec[0]
		fieldKeys[tableName] = tableSpec[1:]
	}
	d.headers = &common.GeneratedDataHeaders{
		TagKeys:   tagNames,
		TagTypes:  tagTypes,
		FieldKeys: fieldKeys,
	}
	return d.headers
}

func extractTagNamesAndTypes(tags []string) ([]string, []string) {
	tagNames := make([]string, len(tags))
	tagTypes := make([]string, len(tags))
	for i, tagWithType := range tags {
		tagAndType := strings.Split(tagWithType, " ")
		if len(tagAndType) != 2 {
			panic("tag header has invalid format")
		}
		tagNames[i] = tagAndType[0]
		tagTypes[i] = tagAndType[1]
	}

	return tagNames, tagTypes
}
