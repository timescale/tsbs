package ceresdb

import (
	"bufio"
	"log"
	"strings"

	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
)

const tagsKey = "tags"

type fileDataSource struct {
	scanner *bufio.Scanner
	//cached headers (should be read only at start of file)
	headers *common.GeneratedDataHeaders
}

func (f fileDataSource) NextItem() data.LoadedPoint {
	ok := f.scanner.Scan()
	if !ok && f.scanner.Err() == nil { // nothing scanned & no error = EOF
		return data.LoadedPoint{}
	} else if !ok {
		log.Fatalf("scan error: %v", f.scanner.Err())
	}
	return data.NewLoadedPoint(f.scanner.Bytes())
}

var fatal = log.Fatalf

func (f *fileDataSource) Headers() *common.GeneratedDataHeaders {
	// headers are read from the input file, and should be read first
	if f.headers != nil {
		return f.headers
	}
	// First N lines are header, with the first line containing the tags
	// and their names, the second through N-1 line containing the column
	// names, and last line being blank to separate from the data
	var tags string
	var cols []string
	i := 0
	for {
		var line string
		ok := f.scanner.Scan()
		if !ok && f.scanner.Err() == nil { // nothing scanned & no error = EOF
			fatal("ended too soon, no tags or cols read")
			return nil
		} else if !ok {
			fatal("scan error: %v", f.scanner.Err())
			return nil
		}
		if i == 0 {
			tags = f.scanner.Text()
			tags = strings.TrimSpace(tags)
		} else {
			line = f.scanner.Text()
			line = strings.TrimSpace(line)
			if len(line) == 0 {
				break
			}
			cols = append(cols, line)
		}
		i++
	}

	tagsarr := strings.Split(tags, ",")
	if tagsarr[0] != tagsKey {
		fatal("input header in wrong format. got '%s', expected 'tags'", tags[0])
	}
	tagNames, tagTypes := extractTagNamesAndTypes(tagsarr[1:])
	fieldKeys := make(map[string][]string)
	for _, tableDef := range cols {
		columns := strings.Split(tableDef, ",")
		tableName := columns[0]
		colNames := columns[1:]
		fieldKeys[tableName] = colNames
	}
	f.headers = &common.GeneratedDataHeaders{
		TagTypes:  tagTypes,
		TagKeys:   tagNames,
		FieldKeys: fieldKeys,
	}
	return f.headers
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
