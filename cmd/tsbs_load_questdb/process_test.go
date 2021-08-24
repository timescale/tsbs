package main

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/timescale/tsbs/pkg/data"
)

func emptyLog(_ string, _ ...interface{}) (int, error) {
	return 0, nil
}

type mockServer struct {
	ln         net.Listener
	listenPort int
}

func mockServerStop(ms *mockServer) {
	ms.ln.Close()
}

func mockServerStart() *mockServer {
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		fatal("Failed to start server listen socket: %s\n", err.Error())
	}
	fmt.Println("Mock TCP server listening on port:", ln.Addr().(*net.TCPAddr).Port)
	ms := &mockServer{
		ln:         ln,
		listenPort: ln.Addr().(*net.TCPAddr).Port,
	}
	go func() {
		for {

			conn, err := ln.Accept()
			if err != nil {
				// listen socket is closed
				return
			}
			go func() {
				data := make([]byte, 512)
				for {
					rc, err := conn.Read(data)
					if err != nil {
						if err != io.EOF {
							fatal("failed to read from connection: ", err.Error())
						}
						return
					}
					fmt.Println(conn, " read ", rc)
				}
			}()
		}
	}()
	return ms
}

func TestProcessorInit(t *testing.T) {
	ms := mockServerStart()
	defer mockServerStop(ms)
	questdbILPBindTo = fmt.Sprintf("127.0.0.1:%d", ms.listenPort)
	printFn = emptyLog
	p := &processor{}
	p.Init(0, false, false)
	p.Close(true)

	p = &processor{}
	p.Init(1, false, false)
	p.Close(true)
}

func TestProcessorProcessBatch(t *testing.T) {
	bufPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
		},
	}
	f := &factory{}
	b := f.New().(*batch)
	pt := data.LoadedPoint{
		Data: []byte("tag1=tag1val,tag2=tag2val col1=0.0,col2=0.0 140\n"),
	}
	b.Append(pt)

	cases := []struct {
		doLoad bool
	}{
		{
			doLoad: false,
		},
		{
			doLoad: true,
		},
	}

	for _, c := range cases {
		fatal = func(format string, args ...interface{}) {
			t.Errorf("fatal called for case %v unexpectedly\n", c)
			fmt.Printf(format, args...)
		}

		ms := mockServerStart()
		questdbILPBindTo = fmt.Sprintf("127.0.0.1:%d", ms.listenPort)

		p := &processor{}
		p.Init(0, true, true)
		mCnt, rCnt := p.ProcessBatch(b, c.doLoad)
		if mCnt != b.metrics {
			t.Errorf("process batch returned less metrics than batch: got %d want %d", mCnt, b.metrics)
		}
		if rCnt != uint64(b.rows) {
			t.Errorf("process batch returned less rows than batch: got %d want %d", rCnt, b.rows)
		}
		p.Close(true)
		mockServerStop(ms)
		time.Sleep(50 * time.Millisecond)
	}
}
