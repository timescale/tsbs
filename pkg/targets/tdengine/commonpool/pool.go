package commonpool

import (
	"sync"
	"unsafe"

	"github.com/silenceper/pool"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/timescale/tsbs/pkg/targets/tdengine/thread"
)

type ConnectorPool struct {
	host     string
	user     string
	password string
	port     int
	pool     pool.Pool
}

func NewConnectorPool(user, password, host string, port int) (*ConnectorPool, error) {
	a := &ConnectorPool{user: user, password: password, host: host, port: port}
	poolConfig := &pool.Config{
		InitialCap:  1,
		MaxCap:      10000,
		MaxIdle:     10000,
		Factory:     a.factory,
		Close:       a.close,
		IdleTimeout: -1,
	}
	p, err := pool.NewChannelPool(poolConfig)
	if err != nil {
		return nil, err
	}
	a.pool = p
	return a, nil
}

func (a *ConnectorPool) factory() (interface{}, error) {
	thread.Lock()
	defer thread.Unlock()
	return wrapper.TaosConnect(a.host, a.user, a.password, "", a.port)
}

func (a *ConnectorPool) close(v interface{}) error {
	if v != nil {
		thread.Lock()
		defer thread.Unlock()
		wrapper.TaosClose(v.(unsafe.Pointer))
	}
	return nil
}

func (a *ConnectorPool) Get() (unsafe.Pointer, error) {
	v, err := a.pool.Get()
	if err != nil {
		return nil, err
	}
	return v.(unsafe.Pointer), nil
}

func (a *ConnectorPool) Put(c unsafe.Pointer) error {
	wrapper.TaosResetCurrentDB(c)
	return a.pool.Put(c)
}

func (a *ConnectorPool) Close(c unsafe.Pointer) error {
	return a.pool.Close(c)
}

func (a *ConnectorPool) Release() {
	a.pool.Release()
}

func (a *ConnectorPool) verifyPassword(password string) bool {
	return password == a.password
}

var connectionMap = sync.Map{}

type Conn struct {
	TaosConnection unsafe.Pointer
	pool           *ConnectorPool
}

func (c *Conn) Put() error {
	return c.pool.Put(c.TaosConnection)
}

func GetConnection(user, password, host string, port int) (*Conn, error) {
	p, exist := connectionMap.Load(user)
	if exist {
		connectionPool := p.(*ConnectorPool)
		if !connectionPool.verifyPassword(password) {
			newPool, err := NewConnectorPool(user, password, host, port)
			if err != nil {
				return nil, err
			}
			connectionPool.Release()
			connectionMap.Store(user, newPool)
			c, err := newPool.Get()
			if err != nil {
				return nil, err
			}
			return &Conn{
				TaosConnection: c,
				pool:           newPool,
			}, nil
		} else {
			c, err := connectionPool.Get()
			if err != nil {
				return nil, err
			}
			return &Conn{
				TaosConnection: c,
				pool:           connectionPool,
			}, nil
		}
	} else {
		newPool, err := NewConnectorPool(user, password, host, port)
		if err != nil {
			return nil, err
		}
		connectionMap.Store(user, newPool)
		c, err := newPool.Get()
		if err != nil {
			return nil, err
		}
		return &Conn{
			TaosConnection: c,
			pool:           newPool,
		}, nil
	}
}
