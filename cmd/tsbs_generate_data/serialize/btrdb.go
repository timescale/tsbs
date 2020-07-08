package serialize

import (
	"fmt"
	"io"
)

type BTrDBSerializer struct{}

func (s *BTrDBSerializer) Serialize(p *Point, w io.Writer) error {
	// prefix = measurement + ',' + tagkv + ' ' + timestamp + ' '
	prefix := make([]byte, 0, 1024)
	prefix = append(prefix, p.measurementName...)

	for i := 0; i < len(p.tagKeys); i++ {
		if p.tagValues[i] == nil {
			continue
		}
		prefix = append(prefix, ',')
		prefix = append(prefix, p.tagKeys[i]...)
		prefix = append(prefix, '=')
		prefix = append(prefix, []byte(fmt.Sprint(p.tagValues[i]))...)
	}

	prefix = append(prefix, '\t')
	prefix = fastFormatAppend(p.timestamp.UTC().UnixNano(), prefix)
	prefix = append(prefix, '\t')

	buf := make([]byte, 0, 64)
	// 把一个点拆分成多个数据对象
	for i := 0; i < len(p.fieldKeys); i++ {
		if p.fieldValues[i] == nil {
			continue
		}

		buf = buf[:0] // 清空未写入的 fieldKey
		buf = append(buf, p.fieldKeys[i]...)
		buf = append(buf, '=')
		val := p.fieldValues[i]
		switch val.(type) {
		case int:
			buf = append(buf, []byte(fmt.Sprint(val.(int)))...)
		case int32:
			buf = append(buf, []byte(fmt.Sprint(val.(int32)))...)
		case int64:
			buf = append(buf, []byte(fmt.Sprint(val.(int64)))...)
		case float32:
			buf = append(buf, []byte(fmt.Sprint(val.(float32)))...)
		case float64:
			buf = append(buf, []byte(fmt.Sprint(val.(float64)))...)
		default:
			// btrdb 只支持数字类型
			continue
		}
		buf = append(buf, '\n')

		if _, err := w.Write(prefix); err != nil {
			return err
		}
		if _, err := w.Write(buf); err != nil {
			return err
		}
	}
	return nil
}
