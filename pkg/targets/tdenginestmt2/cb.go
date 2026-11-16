package tdenginestmt2

/*
#cgo CFLAGS: -I/usr/include
#cgo linux LDFLAGS: -L/usr/lib -ltaos
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>
*/
import "C"
import (
	"unsafe"

	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/driver-go/v3/wrapper/cgo"
)

//export QueryCallback2
func QueryCallback2(p unsafe.Pointer, res *C.TAOS_RES, code C.int) {
	caller := (*(*cgo.Handle)(p)).Value().(wrapper.Caller)
	caller.QueryCall(unsafe.Pointer(res), int(code))
}
