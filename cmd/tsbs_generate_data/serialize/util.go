package serialize

import (
	"fmt"
	"strconv"
)

// Utility function for appending various data types to a byte string
func fastFormatAppend(v interface{}, buf []byte) []byte {
	switch vTyped := v.(type) {
	case int:
		return strconv.AppendInt(buf, int64(vTyped), 10)
	case int64:
		return strconv.AppendInt(buf, vTyped, 10)
	case float64:
		// Why -1 ?
		// From Golang source on genericFtoa (called by AppendFloat): 'Negative precision means "only as much as needed to be exact."'
		// Using this instead of an exact number for precision ensures we preserve the precision passed in to the function, allowing us
		// to use different precision for different use cases.
		return strconv.AppendFloat(buf, vTyped, 'f', -1, 64)
	case float32:
		return strconv.AppendFloat(buf, float64(vTyped), 'f', -1, 32)
	case bool:
		return strconv.AppendBool(buf, vTyped)
	case []byte:
		buf = append(buf, vTyped...)
		return buf
	case string:
		buf = append(buf, vTyped...)
		return buf
	case nil:
		return buf
	default:
		panic(fmt.Sprintf("unknown field type for %#v", v))
	}
}
