package tdenginestmt2

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math"
	"strconv"
	"strings"
	"unsafe"

	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
)

const (
	SuperTableHost        = 0
	SuperTableReadings    = 1
	SuperTableDiagnostics = 2
)

type Serializer struct {
	tmpBuf               *bytes.Buffer
	writeBuf             *bytes.Buffer
	useCase              byte
	scale                uint32
	cpuTableList         []int64
	readingsTableList    []int64
	diagnosticsTableList []int64
	superTable           [3]*Table
}

type Table struct {
	sortColumns     map[string]int
	sortTags        map[string]int
	tagBase         []string
	nullBit         []byte
	colOffset       []int
	colBufferLength int
}

func FastFormat(buf *bytes.Buffer, v interface{}) {
	switch v := v.(type) {
	case int:
		buf.WriteString(strconv.Itoa(v))
	case int64:
		buf.WriteString(strconv.FormatInt(v, 10))
	case float64:
		buf.WriteString(strconv.FormatFloat(v, 'f', -1, 64))
	case float32:
		buf.WriteString(strconv.FormatFloat(float64(v), 'f', -1, 32))
	case bool:
		buf.WriteString(strconv.FormatBool(v))
	case []byte:
		buf.WriteByte('\'')
		buf.Write(v)
		buf.WriteByte('\'')
	case string:
		buf.WriteByte('\'')
		buf.WriteString(v)
		buf.WriteByte('\'')
	case nil:
		buf.WriteString("null")
	default:
		panic(fmt.Sprintf("unknown field type for %#v", v))
	}
}

type tbNameRule struct {
	tag      string
	prefix   string
	nilValue string
}

var tbRule = []*tbNameRule{
	{
		tag:      "hostname",
		nilValue: "host_null",
	},
	{
		tag:      "name",
		prefix:   "r_",
		nilValue: "r_truck_null",
	},
	{
		tag:      "name",
		prefix:   "d_",
		nilValue: "d_truck_null",
	},
}

const (
	CpuCase = 1
	IoTCase = 2
)

/*
 fixed header

| version(1 byte) | case (1 byte) | scale (4 bytes)

*/

func (s *Serializer) Config(config *common.DataGeneratorConfig, w io.Writer) error {
	if config.Scale > math.MaxUint32-1 {
		return fmt.Errorf("scale %d is too large", config.Scale)
	}
	s.scale = uint32(config.Scale)
	useCase := s.prepare(config.BaseConfig.Use)
	if useCase == 0 {
		return fmt.Errorf("use case %s not supported", config.BaseConfig.Use)
	}
	bs := make([]byte, 6)
	bs[0] = 1 // version
	bs[1] = useCase
	binary.LittleEndian.PutUint32(bs[2:], s.scale)
	_, err := w.Write(bs)
	if err != nil {
		return err
	}
	s.useCase = useCase
	switch useCase {
	case CpuCase:
		s.cpuTableList = make([]int64, config.BaseConfig.Scale+1)
	case IoTCase:
		s.readingsTableList = make([]int64, config.BaseConfig.Scale+1)
		s.diagnosticsTableList = make([]int64, config.BaseConfig.Scale+1)
	default:
		return fmt.Errorf("use case %s not supported", config.BaseConfig.Use)
	}
	return nil
}

var cpuTable = &Table{
	sortColumns: map[string]int{
		"usage_user":       1,
		"usage_system":     2,
		"usage_idle":       3,
		"usage_nice":       4,
		"usage_iowait":     5,
		"usage_irq":        6,
		"usage_softirq":    7,
		"usage_steal":      8,
		"usage_guest":      9,
		"usage_guest_nice": 10,
	},
	sortTags: map[string]int{
		"hostname":            0,
		"region":              1,
		"datacenter":          2,
		"rack":                3,
		"os":                  4,
		"arch":                5,
		"team":                6,
		"service":             7,
		"service_version":     8,
		"service_environment": 9,
	},
	tagBase: []string{
		"null",
		"null",
		"null",
		"null",
		"null",
		"null",
		"null",
		"null",
		"null",
		"null",
	},
	nullBit: []byte{0b01111111, 0b11100000},
	colOffset: []int{
		0,
		8,
		16,
		24,
		32,
		40,
		48,
		56,
		64,
		72,
		80,
	},
	colBufferLength: 88,
}
var readingsTable = &Table{
	sortColumns: map[string]int{
		"latitude":         1,
		"longitude":        2,
		"elevation":        3,
		"velocity":         4,
		"heading":          5,
		"grade":            6,
		"fuel_consumption": 7,
	},
	sortTags: map[string]int{
		"name":                     0,
		"fleet":                    1,
		"driver":                   2,
		"model":                    3,
		"device_version":           4,
		"load_capacity":            5,
		"fuel_capacity":            6,
		"nominal_fuel_consumption": 7,
	},
	tagBase: []string{
		"null",
		"null",
		"null",
		"null",
		"null",
		"null",
		"null",
		"null",
	},
	nullBit: []byte{0b01111111},
	colOffset: []int{
		0,
		8,
		16,
		24,
		32,
		40,
		48,
		56,
	},
	colBufferLength: 64,
}
var diagnosticsTable = &Table{
	sortColumns: map[string]int{
		"fuel_state":   1,
		"current_load": 2,
		"status":       3,
	},
	sortTags: map[string]int{
		"name":                     0,
		"fleet":                    1,
		"driver":                   2,
		"model":                    3,
		"device_version":           4,
		"load_capacity":            5,
		"fuel_capacity":            6,
		"nominal_fuel_consumption": 7,
	},
	tagBase: []string{
		"null",
		"null",
		"null",
		"null",
		"null",
		"null",
		"null",
		"null",
	},
	nullBit: []byte{0b01110000},
	colOffset: []int{
		0,
		8,
		16,
		24,
	},
	colBufferLength: 32,
}

func (s *Serializer) prepare(use string) uint8 {
	switch use {
	case common.UseCaseCPUOnly, common.UseCaseCPUSingle:
		s.superTable[SuperTableHost] = cpuTable
		return CpuCase
	case common.UseCaseIoT:
		s.superTable[SuperTableReadings] = readingsTable
		s.superTable[SuperTableDiagnostics] = diagnosticsTable
		return IoTCase
	default:
		return 0
	}
}

const (
	CreateTable = 1
	InsertData  = 2
)

/*

  create table sql
  | length (1 or 2 byte)
  | type (1 byte,1) | table type(1 byte) | table index (uint32 4 bytes) | sql buffer |
*/

/*
  insert data
  | length (1 byte)
  | type (1 byte,2) | table type(1 byte) | table index (uint32 4 bytes)
  | duplicate (bool 1 byte)|
  | is null bit | column data|
*/

func (s *Serializer) Serialize(p *data.Point, w io.Writer) error {
	tmpBuf := s.tmpBuf
	tagRawKeys := p.TagKeys()
	tagRawValues := p.TagValues()
	colRawKeys := p.FieldKeys()
	colRawValues := p.FieldValues()
	superTable := p.MeasurementName()

	var tableList []int64
	superTableType := byte(0)
	switch superTable[0] {
	case 'c':
		superTableType = SuperTableHost
		tableList = s.cpuTableList
	case 'r':
		superTableType = SuperTableReadings
		tableList = s.readingsTableList
	case 'd':
		superTableType = SuperTableDiagnostics
		tableList = s.diagnosticsTableList
	default:
		log.Fatalf("super table: %s not found", superTable)
	}

	// generate sub table name
	rule := tbRule[superTableType]
	fixedName := ""
	for index, value := range tagRawValues {
		if len(fixedName) == 0 && BytesToString(tagRawKeys[index]) == rule.tag {
			str, is := value.(string)
			if is {
				fixedName = str
			}
		}
	}

	subTable := ""
	tbNameIndex := uint32(0)

	if len(fixedName) != 0 {
		if len(rule.prefix) == 0 {
			subTable = fixedName
		} else {
			tmpBuf.WriteString(rule.prefix)
			tmpBuf.WriteString(fixedName)
			subTable = tmpBuf.String()
			tmpBuf.Reset()
		}
		sl := strings.Split(fixedName, "_")
		id, err := strconv.ParseInt(sl[len(sl)-1], 10, 32)
		if err != nil {
			log.Fatalf("table: %s id parse failed", subTable)
		}
		if id < 0 {
			log.Fatalf("table: %s id is negative", subTable)
		}
		if id > int64(s.scale) {
			log.Fatalf("table: %s id is too large", subTable)
		}
		tbNameIndex = uint32(id + 1)
	} else {
		// use nil table name
		subTable = rule.nilValue
		tbNameIndex = 0
	}

	stable := s.superTable[superTableType]

	// check sub table exist
	lastTs := tableList[tbNameIndex]
	if lastTs == 0 {
		var tagValues = make([]string, len(stable.sortTags))
		copy(tagValues, stable.tagBase)
		for index, key := range tagRawKeys {
			tagIndex, exist := stable.sortTags[BytesToString(key)]
			if !exist {
				log.Fatalf("stable: %s tag: %s not found", superTable, key)
			}
			tmpBuf.Reset()
			FastFormat(tmpBuf, tagRawValues[index])
			tagValues[tagIndex] = tmpBuf.String()
		}
		tmpBuf.Reset()
		for index, value := range tagValues {
			tmpBuf.WriteString(value)
			if index != len(tagValues)-1 {
				tmpBuf.WriteByte(',')
			}
		}
		//|length | type (1 byte,1) | table type(1 byte) | table index (uint32 4 bytes) | sql buffer |

		sqlBuf := fmt.Sprintf(" %s using %s tags (%s)", subTable, superTable, tmpBuf.Bytes())
		s.writeBuf.Reset()
		tmpBuf.Reset()
		length := 6 + len(sqlBuf)

		if length < 128 {
			s.writeBuf.WriteByte(byte(length))
		} else {
			s.writeBuf.WriteByte(byte(length&0x7f | 0x80))
			s.writeBuf.WriteByte(byte(length >> 7))
		}

		// type
		s.writeBuf.WriteByte(CreateTable)
		// table type
		s.writeBuf.WriteByte(superTableType)
		// table index
		bs := make([]byte, 4)
		binary.LittleEndian.PutUint32(bs, tbNameIndex)
		s.writeBuf.Write(bs)
		// sql length
		s.writeBuf.WriteString(sqlBuf)
		_, err := w.Write(s.writeBuf.Bytes())
		if err != nil {
			return err
		}
	}
	nullBit := make([]byte, len(stable.nullBit))
	copy(nullBit, stable.nullBit)
	colBuffer := make([]byte, stable.colBufferLength)
	ts := p.TimestampInUnixMs()
	duplicate := false
	tableList[tbNameIndex] = ts
	if ts <= lastTs {
		duplicate = true
	}
	binary.LittleEndian.PutUint64(colBuffer, uint64(ts))
	for index, key := range colRawKeys {
		colIndex, exist := stable.sortColumns[BytesToString(key)]
		if !exist {
			log.Fatalf("stable: %s column: %s not found", superTable, key)
		}
		value := colRawValues[index]
		if value == nil {
			continue
		}
		switch v := value.(type) {
		case int:
			binary.LittleEndian.PutUint64(colBuffer[stable.colOffset[colIndex]:], uint64(v))
		case int64:
			binary.LittleEndian.PutUint64(colBuffer[stable.colOffset[colIndex]:], uint64(v))
		case float64:
			binary.LittleEndian.PutUint64(colBuffer[stable.colOffset[colIndex]:], math.Float64bits(v))
		case float32:
			binary.LittleEndian.PutUint64(colBuffer[stable.colOffset[colIndex]:], math.Float64bits(float64(v)))
		default:
			log.Fatalf("stable: %s column: %s type: %T not supported", superTable, key, v)
		}
		pos := CharOffset(colIndex)
		nullBit[pos] = BMUnSetNull(nullBit[pos], colIndex)
	}
	//insert data
	//| length (1 byte)
	//| type (1 byte,2) | table type(1 byte) | table index (uint32 4 bytes)
	//| duplicate (bool 1 byte)|
	//| is null bit | column data|
	s.writeBuf.Reset()
	length := 7 + len(nullBit) + len(colBuffer)
	if length >= 128 {
		log.Fatalf("length %d is too large", length)
	}
	s.writeBuf.WriteByte(byte(length))
	// type
	s.writeBuf.WriteByte(InsertData)
	// table type
	s.writeBuf.WriteByte(superTableType)
	// table index
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, tbNameIndex)
	s.writeBuf.Write(bs)
	// duplicate
	if duplicate {
		s.writeBuf.WriteByte(1)
	} else {
		s.writeBuf.WriteByte(0)
	}
	// is null bit
	s.writeBuf.Write(nullBit)
	// column data
	s.writeBuf.Write(colBuffer)
	_, err := w.Write(s.writeBuf.Bytes())
	return err
}

func BytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func BitPos(n int) int {
	return n & (7)
}

func CharOffset(n int) int {
	return n >> 3
}

func BMUnSetNull(c byte, n int) byte {
	return c - (1 << (7 - BitPos(n)))
}

const CpuSql = "create stable cpu (" +
	"ts timestamp," +
	"usage_user bigint," +
	"usage_system bigint," +
	"usage_idle bigint," +
	"usage_nice bigint," +
	"usage_iowait bigint," +
	"usage_irq bigint," +
	"usage_softirq bigint," +
	"usage_steal bigint," +
	"usage_guest bigint," +
	"usage_guest_nice bigint) " +
	"tags (" +
	"hostname varchar(30)," +
	"region varchar(30)," +
	"datacenter varchar(30)," +
	"rack varchar(30)," +
	"os varchar(30)," +
	"arch varchar(30)," +
	"team varchar(30)," +
	"service varchar(30)," +
	"service_version varchar(30)," +
	"service_environment varchar(30))"
const ReadingsSql = "CREATE STABLE `readings` (" +
	"`ts` TIMESTAMP, " +
	"`latitude` DOUBLE, " +
	"`longitude` DOUBLE, " +
	"`elevation` DOUBLE, " +
	"`velocity` DOUBLE, " +
	"`heading` DOUBLE, " +
	"`grade` DOUBLE, " +
	"`fuel_consumption` DOUBLE) " +
	"TAGS (" +
	"`name` VARCHAR(30), " +
	"`fleet` VARCHAR(30), " +
	"`driver` VARCHAR(30), " +
	"`model` VARCHAR(30), " +
	"`device_version` VARCHAR(30), " +
	"`load_capacity` DOUBLE, " +
	"`fuel_capacity` DOUBLE, " +
	"`nominal_fuel_consumption` DOUBLE)"

const DiagnosticsSql = "CREATE STABLE `diagnostics` (" +
	"`ts` TIMESTAMP, " +
	"`fuel_state` DOUBLE, " +
	"`current_load` DOUBLE, " +
	"`status` BIGINT) " +
	"TAGS (" +
	"`name` VARCHAR(30), " +
	"`fleet` VARCHAR(30), " +
	"`driver` VARCHAR(30), " +
	"`model` VARCHAR(30), " +
	"`device_version` VARCHAR(30), " +
	"`load_capacity` DOUBLE, " +
	"`fuel_capacity` DOUBLE, " +
	"`nominal_fuel_consumption` DOUBLE)"
