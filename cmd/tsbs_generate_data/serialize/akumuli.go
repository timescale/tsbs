package serialize

import (
	"fmt"
	"io"
)

// AkumuliSerializer writes a series of Point elements into RESP encoded
// buffer.
type AkumuliSerializer struct {
	book       map[string]int
	bookClosed bool
	deffered   []Point
	index      int
}

func (s *AkumuliSerializer) pushDeferred(w io.Writer) (err error) {
	buf := make([]byte, 0, 1024)
	buf = append(buf, "<<Deferred>>\n"...)
	_, err = w.Write(buf)
	return err
}

// Serialize writes Point data to the given writer, conforming to the
// AKUMULI RESP protocol.  Serialized adds extra data to guide data loader.
//
func (s *AkumuliSerializer) Serialize(p *Point, w io.Writer) (err error) {
	if s.book == nil {
		s.book = make(map[string]int)
		s.deffered = make([]Point, 0)
	}

	buf := make([]byte, 0, 1024)

	// Series name
	for i := 0; i < len(p.fieldKeys); i++ {
		buf = append(buf, p.measurementName...)
		buf = append(buf, '.')
		buf = append(buf, p.fieldKeys[i]...)
		if i+1 < len(p.fieldKeys) {
			buf = append(buf, '|')
		} else {
			buf = append(buf, ' ')
		}
	}

	for i := 0; i < len(p.tagKeys); i++ {
		buf = append(buf, ' ')
		buf = append(buf, p.tagKeys[i]...)
		buf = append(buf, '=')
		buf = append(buf, p.tagValues[i]...)
	}

	series := string(buf)
	if !s.bookClosed {
		// Save point for later
		if id, ok := s.book[series]; ok {
			s.pushDeferred(w)
			s.bookClosed = true
			buf = make([]byte, 0, 1024)
			buf = append(buf, fmt.Sprintf(":%d", id)...)
		} else {
			// Shortcut
			s.index++
			tmp := make([]byte, 0, 1024)
			tmp = append(tmp, "*2\n"...)
			tmp = append(tmp, buf...)
			tmp = append(tmp, '\n')
			tmp = append(tmp, fmt.Sprintf(":%d\n", s.index)...)
			_, err := w.Write(tmp)
			s.book[series] = s.index
			return err
		}
	} else {
		// Replace the series name with the value from the book
		if id, ok := s.book[series]; ok {
			buf = buf[:0]
			buf = append(buf, fmt.Sprintf(":%d", id)...)
		}
	}

	buf = append(buf, '\n')

	// Timestamp
	buf = append(buf, '+')
	buf = fastFormatAppend(p.timestamp.UTC().UnixNano(), buf)
	buf = append(buf, '\n')

	// Values
	for i := 0; i < len(p.fieldKeys); i++ {
		v := p.fieldValues[i]
		switch v.(type) {
		case int, int64:
			buf = append(buf, ':')
		case float64:
			buf = append(buf, '+')
		}
		buf = fastFormatAppend(v, buf)
		buf = append(buf, '\n')
	}

	_, err = w.Write(buf)

	return err
}

func (s *AkumuliSerializer) Close(w io.Writer) (err error) {
	buf := make([]byte, 0, 1024)
	buf = append(buf, '%')
	_, err = w.Write(buf)
	return err
}
