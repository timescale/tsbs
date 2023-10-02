package ceresdb

import "github.com/CeresDB/ceresdb-client-go/ceresdb"

func NewClient(endpoint string, accessMode string, opts ...ceresdb.Option) (ceresdb.Client, error) {
	mode := ceresdb.Direct
	if accessMode == "proxy" {
		mode = ceresdb.Proxy
	}
	return ceresdb.NewClient(endpoint, mode, opts...)
}
