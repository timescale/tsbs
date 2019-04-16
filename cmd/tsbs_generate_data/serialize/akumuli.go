package serialize

import (
	"fmt"
	"io"
	"unsafe"
)

// AkumuliSerializer writes a series of Point elements into RESP encoded
// buffer.
type AkumuliSerializer struct {
	book       map[string]int
	bookClosed bool
	deferred   []byte
	index      int
}

// Serialize writes Point data to the given writer, conforming to the
// AKUMULI RESP protocol.  Serialized adds extra data to guide data loader.
//
func (s *AkumuliSerializer) Serialize(p *Point, w io.Writer) (err error) {
	if s.book == nil {
		s.book = make(map[string]int)
		s.deferred = make([]byte, 0, 4096)
		s.bookClosed = false
	}

	deferPoint := false

	buf := make([]byte, 0, 1024)
	// Add cue
	const HeaderLength = 6
	buf = append(buf, "AAAAFF+"...)

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
	if !s.bookClosed {
		// Save point for later
		if id, ok := s.book[series]; ok {
			s.bookClosed = true
			_, err = w.Write(s.deferred)
			if err != nil {
				return err
			}
			buf = buf[:HeaderLength]
			buf = append(buf, fmt.Sprintf(":%d", id)...)
			pid := (*[4]byte)(unsafe.Pointer(&id))
			copy(buf[:4], pid[:])
		} else {
			// Shortcut
			s.index++
			tmp := make([]byte, 0, 1024)
			tmp = append(tmp, "AAAAFF*2\n"...)
			tmp = append(tmp, buf[HeaderLength:]...)
			tmp = append(tmp, '\n')
			tmp = append(tmp, fmt.Sprintf(":%d\n", s.index)...)
			s.book[series] = s.index
			// Update cue
			tmplen := (uint16)(len(tmp))
			plen := (*[2]byte)(unsafe.Pointer(&tmplen))
			copy(tmp[4:HeaderLength], plen[:])
			pid := (*[4]byte)(unsafe.Pointer(&s.index))
			copy(tmp[:4], pid[:])
			copy(buf[:4], pid[:])
			_, err = w.Write(tmp)
			if err != nil {
				return err
			}
			deferPoint = true
			buf = buf[:HeaderLength]
			buf = append(buf, fmt.Sprintf(":%d", s.index)...)
		}
	} else {
		// Replace the series name with the value from the book
		if id, ok := s.book[series]; ok {
			buf = buf[:HeaderLength]
			buf = append(buf, fmt.Sprintf(":%d", id)...)
			pid := (*[4]byte)(unsafe.Pointer(&id))
			copy(buf[:4], pid[:])
		} else {
			panic("Unexpected series name")
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

	// Update cue
	buflen := (uint16)(len(buf))
	plen := (*[2]byte)(unsafe.Pointer(&buflen))
	copy(buf[4:HeaderLength], plen[:])
	if deferPoint {
		s.deferred = append(s.deferred, buf...)
		err = nil
	} else {
		_, err = w.Write(buf)
	}

	return err
}
