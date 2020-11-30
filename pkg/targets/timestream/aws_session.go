package timestream

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"golang.org/x/net/http2"
	"net"
	"net/http"
	"time"
)

func OpenAWSSession(awsRegion *string, timeout time.Duration) (*session.Session, error) {
	tr := &http.Transport{
		ResponseHeaderTimeout: 20 * time.Second,
		// Using DefaultTransport values for other parameters: https://golang.org/pkg/net/http/#RoundTripper
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			KeepAlive: 30 * time.Second,
			Timeout:   timeout,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	if err := http2.ConfigureTransport(tr); err != nil {
		panic("could not configure http transport: " + err.Error())

	}
	return session.NewSession(&aws.Config{
		Region:     awsRegion,
		MaxRetries: aws.Int(10),
		HTTPClient: &http.Client{Transport: tr}})
}
