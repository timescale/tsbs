package tdengine

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/timescale/tsbs/pkg/data"
)

type Serializer struct {
	tmpBuf     *bytes.Buffer
	tableMap   map[string]struct{}
	superTable map[string]*Table
}

var nothing = struct{}{}

type Table struct {
	columns map[string]struct{}
	tags    map[string]struct{}
}

func FastFormat(buf *bytes.Buffer, v interface{}) string {
	switch v.(type) {
	case int:
		buf.WriteString(strconv.Itoa(v.(int)))
		return "bigint"
	case int64:
		buf.WriteString(strconv.FormatInt(v.(int64), 10))
		return "bigint"
	case float64:
		buf.WriteString(strconv.FormatFloat(v.(float64), 'f', -1, 64))
		return "double"
	case float32:
		buf.WriteString(strconv.FormatFloat(float64(v.(float32)), 'f', -1, 32))
		return "double"
	case bool:
		buf.WriteString(strconv.FormatBool(v.(bool)))
		return "bool"
	case []byte:
		buf.WriteByte('\'')
		buf.WriteString(string(v.([]byte)))
		buf.WriteByte('\'')
		return "binary(30)"
	case string:
		buf.WriteByte('\'')
		buf.WriteString(v.(string))
		buf.WriteByte('\'')
		return "binary(30)"
	case nil:
		buf.WriteString("null")
		return "null"
	default:
		panic(fmt.Sprintf("unknown field type for %#v", v))
	}
}

var tmpMD5 = map[string]string{}
var tmpIndex = 0

func calculateTable(src []byte) string {
	key := string(src)
	v, exist := tmpMD5[key]
	if exist {
		return v
	}
	tmpIndex += 1
	v = fmt.Sprintf("t_%d", tmpIndex)
	tmpMD5[key] = v
	return v
}

const (
	Insert         = '1'
	CreateSTable   = '2'
	CreateSubTable = '3'
	Modify         = '4'
)

type tbNameRule struct {
	tag      string
	prefix   string
	nilValue string
}

var tbRuleMap = map[string]*tbNameRule{
	"cpu": {
		tag:      "hostname",
		nilValue: "host_null",
	},
	"readings": {
		tag:      "name",
		prefix:   "r_",
		nilValue: "r_truck_null",
	},
	"diagnostics": {
		tag:      "name",
		prefix:   "d_",
		nilValue: "d_truck_null",
	},
}

func (s *Serializer) Serialize(p *data.Point, w io.Writer) error {
	var fieldKeys []string
	var fieldValues []string
	var fieldTypes []string
	var tagValues []string
	var tagKeys []string
	var tagTypes []string
	tKeys := p.TagKeys()
	tValues := p.TagValues()
	fKeys := p.FieldKeys()
	fValues := p.FieldValues()
	superTable := string(p.MeasurementName())
	for i, value := range fValues {
		fType := FastFormat(s.tmpBuf, value)
		fieldKeys = append(fieldKeys, convertKeywords(string(fKeys[i])))
		fieldTypes = append(fieldTypes, fType)
		fieldValues = append(fieldValues, s.tmpBuf.String())
		s.tmpBuf.Reset()
	}

	rule := tbRuleMap[superTable]
	fixedName := ""
	for i, value := range tValues {
		tType := FastFormat(s.tmpBuf, value)
		if rule != nil && len(fixedName) == 0 && string(tKeys[i]) == rule.tag {
			str, is := value.(string)
			if is {
				fixedName = str
			}
		}
		tagKeys = append(tagKeys, convertKeywords(string(tKeys[i])))
		tagTypes = append(tagTypes, tType)
		tagValues = append(tagValues, s.tmpBuf.String())
		s.tmpBuf.Reset()
	}

	subTable := ""
	if rule != nil {
		if len(fixedName) != 0 {
			if len(rule.prefix) == 0 {
				subTable = fixedName
			} else {
				s.tmpBuf.WriteString(rule.prefix)
				s.tmpBuf.WriteString(fixedName)
				subTable = s.tmpBuf.String()
				s.tmpBuf.Reset()
			}
		} else {
			subTable = rule.nilValue
		}
	} else {
		s.tmpBuf.WriteString(superTable)
		for i, v := range tagValues {
			s.tmpBuf.WriteByte(',')
			s.tmpBuf.WriteString(tagKeys[i])
			s.tmpBuf.WriteByte('=')
			s.tmpBuf.WriteString(v)
		}
		subTable = calculateTable(s.tmpBuf.Bytes())
		s.tmpBuf.Reset()
	}
	stable, exist := s.superTable[superTable]
	if !exist {
		for i := 0; i < len(fieldTypes); i++ {
			s.tmpBuf.WriteByte(',')
			s.tmpBuf.WriteString(fieldKeys[i])
			s.tmpBuf.WriteByte(' ')
			s.tmpBuf.WriteString(fieldTypes[i])
		}
		fieldStr := s.tmpBuf.String()
		s.tmpBuf.Reset()
		for i := 0; i < len(tagTypes); i++ {
			s.tmpBuf.WriteString(tagKeys[i])
			s.tmpBuf.WriteByte(' ')
			s.tmpBuf.WriteString(tagTypes[i])
			if i != len(tagTypes)-1 {
				s.tmpBuf.WriteByte(',')
			}
		}
		tagStr := s.tmpBuf.String()
		fmt.Fprintf(w, "%c,%s,%s,create table %s (ts timestamp%s) tags (%s)\n", CreateSTable, superTable, subTable, superTable, fieldStr, tagStr)
		s.tmpBuf.Reset()
		table := &Table{
			columns: map[string]struct{}{},
			tags:    map[string]struct{}{},
		}
		for _, key := range fieldKeys {
			table.columns[key] = nothing
		}
		for _, key := range tagKeys {
			table.tags[key] = nothing
		}
		s.superTable[superTable] = table
	} else {
		for _, key := range fieldKeys {
			if _, exist = stable.columns[key]; !exist {
				fmt.Fprintf(w, "%c,%s,%s,alter table %s add COLUMN %s double\n", Modify, superTable, subTable, superTable, key)
				stable.columns[key] = nothing
			}
		}
		for _, key := range tagKeys {
			if _, exist = stable.tags[key]; !exist {
				fmt.Fprintf(w, "%c,%s,%s,alter table %s add TAG %s binary(30)\n", Modify, superTable, subTable, superTable, key)
				stable.tags[key] = nothing
			}
		}
	}
	_, exist = s.tableMap[subTable]
	if !exist {
		fmt.Fprintf(w, "%c,%s,%s,create table %s using %s (%s) tags (%s)\n", CreateSubTable, superTable, subTable, subTable, superTable, strings.Join(tagKeys, ","), strings.Join(tagValues, ","))
		s.tableMap[subTable] = nothing
	}

	fmt.Fprintf(w, "%c,%s,%d,(%d,%s)\n", Insert, subTable, len(fieldValues), p.TimestampInUnixMs(), strings.Join(fieldValues, ","))
	return nil
}

var keyWords = map[string]bool{
	"port": true,
}

func convertKeywords(s string) string {
	if is := keyWords[s]; is {
		return fmt.Sprintf("`%s`", s)
	}
	return s
}
