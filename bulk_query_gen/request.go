package main

import (
	"net/url"
)

type Request struct {
	Method         string
	Path           string
	QueryArguments url.Values
	Body           string
}
