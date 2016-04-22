package main

import (
	"flag"
	"log"

	"github.com/valyala/fasthttp"
)

var (
	addr = flag.String("addr", ":8080", "TCP address to listen to")
)

var body = []byte("OK")

func main() {
	flag.Parse()

	if err := fasthttp.ListenAndServe(*addr, requestHandler); err != nil {
		log.Fatalf("Error in ListenAndServe: %s", err)
	}
}

func requestHandler(ctx *fasthttp.RequestCtx) {
	_, err := ctx.Write(body)
	if err != nil {
		log.Fatal(err)
	}
}
