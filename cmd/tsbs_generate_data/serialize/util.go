package serialize

import (
	"fmt"
	"strconv"
)

// Utility function for appending various data types to a byte string
func fastFormatAppend(v interface{}, buf []byte) []byte {
	switch v.(type) {
	case int:
		return strconv.AppendInt(buf, int64(v.(int)), 10)
	case int64:
		return strconv.AppendInt(buf, v.(int64), 10)
	case float64:
		// Why -1 ?
		// From Golang source on genericFtoa (called by AppendFloat): 'Negative precision means "only as much as needed to be exact."'
		// Using this instead of an exact number for precision ensures we preserve the precision passed in to the function, allowing us
		// to use different precision for different use cases.
		return strconv.AppendFloat(buf, v.(float64), 'f', -1, 64)
	case float32:
		return strconv.AppendFloat(buf, float64(v.(float32)), 'f', -1, 32)
	case bool:
		return strconv.AppendBool(buf, v.(bool))
	case []byte:
		buf = append(buf, v.([]byte)...)
		return buf
	case string:
		buf = append(buf, v.(string)...)
		return buf
	case nil:
		return buf
	default:
		panic(fmt.Sprintf("unknown field type for %#v", v))
	}
}
