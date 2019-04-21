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
	closeChan      chan int
	addr           string
}

// Create new connection
func createTsdb(addr string, connectTimeout, writeTimeout time.Duration) *tsdbConn {
	conn := new(tsdbConn)
	conn.writeTimeout = writeTimeout
	conn.connectTimeout = connectTimeout
	conn.writeChan = make(chan []byte, 100)
	conn.closeChan = make(chan int, 10)
	conn.connected = false
	conn.addr = addr
	err := conn.connect()
	if err != nil {
		log.Println("Error establishing connection", err)
	}
	return conn
}

func (conn *tsdbConn) connect() error {
	c, err := net.Dial("tcp", conn.addr)
	if err == nil {
		conn.conn = c
		conn.connected = true
		go conn.run()
		log.Println("Connection with", conn.addr, "successful")
	} else {
		conn.connected = false
		log.Println("Can't establish connection with", conn.addr)
	}
	return err
}

func (conn *tsdbConn) write(data []byte) error {
	deadline := time.Now()
	deadline.Add(conn.writeTimeout)
	_, err := conn.conn.Write(data)
	return err
}

func (conn *tsdbConn) Write(data []byte) {
	conn.writeChan <- data
}

func (conn *tsdbConn) Close() {
	conn.writeChan <- nil
	// Wait for completion
	_ = <-conn.closeChan
}

func (conn *tsdbConn) startReadAsync() {
	buffer, err := ioutil.ReadAll(conn.conn)
	if err != nil {
		log.Println("Read error", err)
	} else {
		log.Println("Database returned error:", string(buffer))
	}
	conn.writeChan <- nil // To stop the goroutine
}

func (conn *tsdbConn) run() {
	for {
		buf := <-conn.writeChan
		if buf == nil {
			conn.conn.Close()
			conn.connected = false
			conn.closeChan <- 0
			break
		}
		err := conn.write(buf)
		if err != nil {
			log.Println("TSDB write error:", err)
			conn.conn.Close()
			conn.connected = false
			conn.closeChan <- 0
			break
		}
	}
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
		if tsdb.pool[i].connected == false {
			panic("Connection error")
		}
	}
	return tsdb
}

func (tsdb *tsdbConnPool) Write(shardid uint32, buf []byte) {
	tsdb.pool[shardid%tsdb.size].Write(buf)
}

func (tsdb *tsdbConnPool) Close() {
	for _, val := range tsdb.pool {
		val.Close()
	}
}
