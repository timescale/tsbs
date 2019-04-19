package main

import (
	"io/ioutil"
	"log"
	"net"
	"time"
)

type tsdbConn struct {
	conn           net.Conn
	writeTimeout   time.Duration
	connectTimeout time.Duration
	connected      bool
	writeChan      chan []byte
	reconnectChan  chan int
	addr           string
}

// Create new connection
func createTsdb(addr string, connectTimeout, writeTimeout time.Duration) *tsdbConn {
	conn := new(tsdbConn)
	conn.writeTimeout = writeTimeout
	conn.connectTimeout = connectTimeout
	conn.writeChan = make(chan []byte, 100)
	conn.reconnectChan = make(chan int, 10)
	conn.connected = false
	conn.addr = addr
	conn.reconnectChan <- 0
	go conn.reconnect()
	return conn
}

func (conn *tsdbConn) connect() error {
	c, err := net.Dial("tcp", conn.addr)
	if err == nil {
		conn.conn = c
		conn.connected = true
	} else {
		conn.connected = false
	}
	return err
}

func (conn *tsdbConn) write(data []byte) error {
	deadline := time.Now()
	deadline.Add(conn.writeTimeout)
	_, err := conn.conn.Write(data)
	return err
}

func (conn *tsdbConn) Close() {
	conn.reconnectChan <- -1
	conn.writeChan <- nil
	if conn.connected {
		conn.conn.Close()
		conn.connected = false
	}
}

func (conn *tsdbConn) startReadAsync() {
	buffer, err := ioutil.ReadAll(conn.conn)
	if err != nil {
		log.Println("Read error", err)
	} else {
		log.Println("Database returned error:", string(buffer))
	}
	conn.writeChan <- nil // To stop the goroutine
	conn.conn.Close()
	conn.connected = false
	conn.reconnectChan <- 0
}

func (conn *tsdbConn) run() {
	for {
		buf := <-conn.writeChan
		if buf == nil {
			break
		}
		err := conn.write(buf)
		if err != nil {
			log.Println("TSDB write error:", err)
			break
		}
	}
}

func (conn *tsdbConn) reconnect() {
	for {
		ix := <-conn.reconnectChan
		if ix < 0 {
			log.Println("Reconnection job stopping")
			break
		}
		if conn.connected {
			conn.conn.Close()
			conn.connected = false
			time.Sleep(conn.connectTimeout)
		}
		err := conn.connect()
		if err != nil {
			log.Println("TSDB connection error", err)
			conn.reconnectChan <- (ix + 1)
		} else {
			log.Println("Connection attempt successful")
			go conn.run()
			go conn.startReadAsync()
		}
	}
}

func (conn *tsdbConn) Write(data []byte) {
	conn.writeChan <- data
}

// TSDB connection pool with affinity
type tsdbConnPool struct {
	pool           []*tsdbConn
	size           uint32
	targetAddr     string
	writeTimeout   time.Duration
	connectTimeout time.Duration
}

func createTsdbPool(size uint32, targetAddr string, connectTimeout, writeTimeout time.Duration) *tsdbConnPool {
	tsdb := new(tsdbConnPool)
	tsdb.size = size
	tsdb.pool = make([]*tsdbConn, size, size)
	tsdb.connectTimeout = connectTimeout
	tsdb.writeTimeout = writeTimeout
	tsdb.targetAddr = targetAddr
	// Init pool
	for i := uint32(0); i < size; i++ {
		tsdb.pool[i] = createTsdb(tsdb.targetAddr, tsdb.connectTimeout, tsdb.writeTimeout)
	}
	return tsdb
}

func (tsdb *tsdbConnPool) Write(shardid uint32, buf []byte) {
	tsdb.pool[shardid%tsdb.size].conn.Write(buf)
}

func (tsdb *tsdbConnPool) Close() {
	for _, val := range tsdb.pool {
		val.Close()
	}
}
