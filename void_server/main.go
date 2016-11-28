package main

import (
	"flag"
	"log"

	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/reuseport"
)

var (
	addr = flag.String("addr", ":8080", "TCP address to listen to")
)

var body = []byte("\n")

func main() {
	flag.Parse()

	ln, err := reuseport.Listen("tcp4", *addr)
	if err != nil {
		log.Fatal(err)
	}

	if err := fasthttp.Serve(ln, defaultRequestHandler); err != nil {
		log.Fatalf("Error in ListenAndServe: %s", err)
	}
}

func defaultRequestHandler(ctx *fasthttp.RequestCtx) {
	ctx.SetStatusCode(fasthttp.StatusNoContent)
}
