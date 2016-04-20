package main

import (
	"net/url"
)

type Query struct {
	Method    string
	Path      string
	Arguments url.Values
	Body      string
}
