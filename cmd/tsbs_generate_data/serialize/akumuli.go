package serialize

import (
	"fmt"
	"io"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
)

// AkumuliSerializer writes a series of Point elements into RESP encoded
// buffer.
type AkumuliSerializer struct {
	book       map[string]int
	bookClosed bool
	deferred   []*Point
	index      int
	first      bool
}

func (s *AkumuliSerializer) pushDeferred(w io.Writer) (err error) {
	fmt.Println("pushDeferred start")
	for _, point := range s.deferred {
		err = s.Serialize(point, w)
		if err != nil {
			return err
		}
	}
	fmt.Println("pushDeferred stop")
	return err
}

// Deep-copy pont
func (s *AkumuliSerializer) deferPoint(p *Point) {
	newp := serialize.NewPoint()
	newp.SetTimestamp(p.timestamp)
	newp.SetMeasurementName(p.measurementName)
	for index, key := range p.TagKeys {
		newp.AppendTag(key, p.tagValues[index])
	}
	for index, key := range p.fieldKeys {
		newp.AppendField(key, p.fieldValues[index])
	}
	s.deferred = append(s.deferred, newp)
}

// Serialize writes Point data to the given writer, conforming to the
// AKUMULI RESP protocol.  Serialized adds extra data to guide data loader.
//
func (s *AkumuliSerializer) Serialize(p *Point, w io.Writer) (err error) {
	if s.book == nil {
		s.book = make(map[string]int)
		s.deferred = make([]*Point, 0)
		s.bookClosed = false
	}
	if !s.first {
		s.first = true
		fmt.Println(p.timestamp.UTC().UnixNano())
	}

	buf := make([]byte, 0, 1024)
	// Add cue
	const HeaderLength = 12
	buf = append(buf, "AAAAAAAAFFFF+"...)

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

	series := string(buf[HeaderLength:])
	fmt.Println("-----series:", series)
	if !s.bookClosed {
		// Save point for later
		if id, ok := s.book[series]; ok {
			s.deferPoint(p)
			s.bookClosed = true
			s.pushDeferred(w)
			buf = make([]byte, 0, 1024)
			buf = append(buf, fmt.Sprintf("AAAAAAAAFFFF:%d", id)...)
		} else {
			// Shortcut
			s.index++
			tmp := make([]byte, 0, 1024)
			tmp = append(tmp, "AAAAAAAAFFFF*2\n"...)
			tmp = append(tmp, buf[HeaderLength:]...)
			tmp = append(tmp, '\n')
			tmp = append(tmp, fmt.Sprintf(":%d\n", s.index)...)
			_, err := w.Write(tmp)
			s.book[series] = s.index
			s.deferPoint(p)
			return err
		}
	} else {
		// Replace the series name with the value from the book
		if id, ok := s.book[series]; ok {
			buf = buf[:HeaderLength]
			buf = append(buf, fmt.Sprintf(":%d", id)...)
			fmt.Println("--------in book", id)
		} else {
			fmt.Println("--------off book", series)
		}
	}

	buf = append(buf, '\n')

	// Timestamp
	buf = append(buf, ':')
	buf = fastFormatAppend(p.timestamp.UTC().UnixNano(), buf)
	buf = append(buf, '\n')

	// Values
	buf = append(buf, fmt.Sprintf("*%d\n", len(p.fieldValues))...)
	for i := 0; i < len(p.fieldValues); i++ {
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
