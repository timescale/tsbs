package timestream

import (
	"bufio"
	"fmt"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"log"
	"strconv"
	"strings"
	"time"
)

const (
	tagsKey = "tags"
)

type fileDataSource struct {
	_headers     *common.GeneratedDataHeaders
	scanner      *bufio.Scanner
	useCurrentTs bool
}

func (f *fileDataSource) Headers() *common.GeneratedDataHeaders {
	// headers are read from the input file, and should be read first
	if f._headers != nil {
		return f._headers
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
			log.Fatal("reading headers ended too soon, no tags or cols read")
			return nil
		} else if !ok {
			log.Fatalf("scan error: %v", f.scanner.Err())
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
		log.Fatalf("input header in wrong format. got '%s', expected '%s'", string(tags[0]), tagsKey)
		return nil
	}
	tagNames, tagTypes, err := extractTagNamesAndTypes(tagsarr[1:])
	if err != nil {
		log.Fatal(err)
		return nil
	}
	fieldKeys := make(map[string][]string)
	for _, tableDef := range cols {
		columns := strings.Split(tableDef, ",")
		tableName := columns[0]
		colNames := columns[1:]
		fieldKeys[tableName] = colNames
	}
	f._headers = &common.GeneratedDataHeaders{
		TagTypes:  tagTypes,
		TagKeys:   tagNames,
		FieldKeys: fieldKeys,
	}
	return f._headers
}

func (f *fileDataSource) NextItem() data.LoadedPoint {
	if f._headers == nil {
		log.Fatal("headers not read before starting to decode points")
		return data.LoadedPoint{}
	}
	ok := f.scanner.Scan()
	if !ok && f.scanner.Err() == nil { // nothing scanned & no error = EOF
		return data.LoadedPoint{}
	} else if !ok {
		log.Fatalf("scan error: %v", f.scanner.Err())
		return data.LoadedPoint{}
	}

	// The first line is a CSV line of tags with the first element being "tags"
	parts := strings.SplitN(f.scanner.Text(), ",", 2) // prefix & then rest of line
	prefix := parts[0]
	if prefix != tagsKey {
		log.Fatalf("data file in invalid format; got %s expected %s", prefix, tagsKey)
		return data.LoadedPoint{}
	}
	newPoint := &deserializedPoint{}
	tagKeys, tagValues := tagsLineToTagValues(parts[1])
	newPoint.tagKeys = tagKeys
	newPoint.tags = tagValues

	// Scan again to get the data line
	ok = f.scanner.Scan()
	if !ok {
		log.Fatalf("scan error: %v", f.scanner.Err())
		return data.LoadedPoint{}
	}
	parts = strings.SplitN(f.scanner.Text(), ",", 2) // prefix & then rest of line
	newPoint.table = parts[0]
	ts, fields := fieldsLineToFieldValues(parts[1])
	newPoint.timeUnixNano = f.prepareTimestamp(ts)
	newPoint.fields = fields

	return data.NewLoadedPoint(&newPoint)
}

func (f *fileDataSource) prepareTimestamp(pointTs string) string {
	if !f.useCurrentTs {
		return pointTs
	} else {
		return strconv.FormatInt(time.Now().UnixNano(), 10)
	}
}

func extractTagNamesAndTypes(tags []string) ([]string, []string, error) {
	tagNames := make([]string, len(tags))
	tagTypes := make([]string, len(tags))
	for i, tagWithType := range tags {
		tagAndType := strings.Split(tagWithType, " ")
		if len(tagAndType) != 2 {
			return nil, nil, fmt.Errorf("tag header has invalid format")
		}
		tagNames[i] = tagAndType[0]
		tagTypes[i] = tagAndType[1]
	}

	return tagNames, tagTypes, nil
}

func tagsLineToTagValues(tagsLine string) (tagKeys, tagValues []string) {
	tagsLineSplit := strings.Split(tagsLine, ",")
	tagKeys = make([]string, len(tagsLineSplit))
	tagValues = make([]string, len(tagsLineSplit))
	for i := 0; i < len(tagsLineSplit); i++ {
		parts := strings.Split(tagsLineSplit[i], "=")
		tagKeys[i] = parts[0]
		tagValues[i] = parts[1]
	}
	return tagKeys, tagValues
}

func fieldsLineToFieldValues(fieldsLine string) (time string, fieldValues []*string) {
	metrics := strings.Split(fieldsLine, ",")
	fieldValues = make([]*string, len(metrics)-1)
	// use nil at 2nd position as placeholder for tagKey
	for i, v := range metrics[1:] {
		if v == "" {
			fieldValues[i] = nil
			continue
		}

		fieldValues[i] = &v
	}

	return metrics[0], fieldValues
}
