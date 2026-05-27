package thread

import "runtime"

type Locker struct {
	c chan struct{}
}

func NewLocker(count int) *Locker {
	return &Locker{c: make(chan struct{}, count)}
}

func (l *Locker) Lock() {
	l.c <- struct{}{}
}

func (l *Locker) Unlock() {
	<-l.c
}

var c chan struct{}

func Lock() {
	c <- struct{}{}
}

func Unlock() {
	<-c
}

func init() {
	c = make(chan struct{}, runtime.NumCPU())
}
