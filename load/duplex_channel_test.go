package load

import (
	"testing"
)

func TestNewDuplexChannel(t *testing.T) {
	cases := []struct {
		desc      string
		queueSize int
	}{
		{
			desc:      "queue size 0",
			queueSize: 0,
		},
		{
			desc:      "queue size 1",
			queueSize: 1,
		},
		{
			desc:      "queue size 5",
			queueSize: 5,
		},
	}

	for _, c := range cases {
		ch := newDuplexChannel(c.queueSize)
		if cap(ch.toScanner) != c.queueSize {
			t.Errorf("%s: toScanner channel cap incorrect: got %d want %d", c.desc, cap(ch.toScanner), c.queueSize)
		}
		if cap(ch.toWorker) != c.queueSize {
			t.Errorf("%s: toWorker channel cap incorrect: got %d want %d", c.desc, cap(ch.toScanner), c.queueSize)
		}
	}
}

func TestSendToWorker(t *testing.T) {
	ch := newDuplexChannel(1)
	ch.sendToWorker(&testBatch{})
	if res, ok := <-ch.toWorker; !ok || res == nil {
		t.Errorf("sendToWorker did not send item or sent nil")
	}
}

func TestSendToScanner(t *testing.T) {
	ch := newDuplexChannel(1)
	ch.sendToScanner()
	if res, ok := <-ch.toScanner; !res || !ok {
		t.Errorf("sendToScanner did not send 'true', sent %v", res)
	}
}

func TestClose(t *testing.T) {
	ch := newDuplexChannel(1)
	ch.close()
	_, ok := <-ch.toWorker
	if ok {
		t.Errorf("close did not close toWorker")
	}
	_, ok = <-ch.toScanner
	if ok {
		t.Errorf("close did not close toScanner")
	}
}
