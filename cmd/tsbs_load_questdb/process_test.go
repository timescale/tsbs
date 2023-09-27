package main

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"math/rand"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/timescale/tsbs/pkg/data"
)

const testTlsCertificate = `-----BEGIN CERTIFICATE-----
MIIFETCCAvkCFAjRNUAVvcoaUkmcOSVKxT8cVI1sMA0GCSqGSIb3DQEBCwUAMEUx
CzAJBgNVBAYTAkJHMRMwEQYDVQQIDApTb21lLVN0YXRlMSEwHwYDVQQKDBhJbnRl
cm5ldCBXaWRnaXRzIFB0eSBMdGQwHhcNMjMwOTI2MDcwNDQwWhcNMjQwOTI1MDcw
NDQwWjBFMQswCQYDVQQGEwJCRzETMBEGA1UECAwKU29tZS1TdGF0ZTEhMB8GA1UE
CgwYSW50ZXJuZXQgV2lkZ2l0cyBQdHkgTHRkMIICIjANBgkqhkiG9w0BAQEFAAOC
Ag8AMIICCgKCAgEAlVIxz9icPSBzW72xeDtoaAGnWwlBQGR9ekAOLZGOSZpihrES
W0HdvaQberJqjPWEVhfKGyUPXhvoEFOkq9r5hEskehJZ89u8qHbF1RS9cVECmcil
AOYaooC1pleZd0gqiClt1oFczJBzSL4qOsJS9AM9FGTYO2hZTegPR0Yl69OFGP5V
DAUCEmbfipGaGenYfIe90l9e8tvMh446PIyHKIivN8fP1hBDgY7V2qNVneAqzWyG
Yv36sK3Noeoi1qbGEdAqlZcB29wRz41mRNsRHsn4kZbMPPUPbtbDhciIaDgz7EVo
502XTPr9KJcwGfpVPhV4/O4Ccs7cM9bJqL/dmtC+cPEgWJYJF/9H8QzCaODyd/rK
1TuCxMDHYnYu018hgzdWstdZsZL0o6/qbbVvn4OpTGA4Pqk5KEfawxHiZicNEslP
4hWb26/Fvei2pnidZ+n8pdUIqPh+TpCdYlecbcXTV9oPJSCijDjr7ADKg7y14kSF
vZ1eHHhj/8r5f/KPDPPwmCxwuoiJQ0rnoaJ/gEt0k3XrSNveYLK88RMHExm07ruV
JxLOHz+q6pDrwdMoDS04wT00Q2lscSsEmBqdwiClep8mB+cdwEzPMatt08bQ8iQm
JWDIlzU/ZBkpeaNQA9W+rNGWwuD1LikS7wtZDi/tPaEcJQYmCFkzKymOyvECAwEA
ATANBgkqhkiG9w0BAQsFAAOCAgEABsivUk0dJEZTONdt1B9SfdMHmHiW/PdPyaEX
i9Jg4mw7OfdT7mI8sIqfLtn15WRylmtEOnQkVDRRvQZtP2V1PXh66EmxQm3uXWb9
Uk65Caz7dDmz/fYMt37cgO780v6IoDMy43MCbgKgvGvo6TicmoP/PaJmSk7k+HmA
PdCsTilIoiHlg+nDjW2oWMqhRdJ3cjk6CMsg5UTncorD7/EuwkFyYC6o8YJDdmwI
F9o/8KQOcSS4JIcsyrnDxyVHZL7wKnkSWfENl5dFr1FviSiVBWPkdXkv948zQWfA
pzUgD4q2gRGNWuEoDPD0JNUo2ID6YAwHby40RB43FqM42BqswFnAkz+IwC7vbkHU
7kt4HuaSU2CeuSh3dGDy4NAYhRuDiBZjMEb9p97/DnPKmE0qCueBPf50momHfD9t
y9uKc59wCDCEaGypHgyCrrKe3fqcW6kvL0HTxlC1j9zWA16xhF8O5HObKGdCVkjM
RY2LDpFlgOKmjSgUTBNgPI0boMgJSowEl7h7crtJ+gdwQeGdKcPShIjwU29CrWee
X0mrhAZejQ+QL6O1vWcWix93AFHujw7TQ5xGNyGa9ljhbAEwSEtlsiKhFE1KUe3U
ZzmgzGSOpl/Fq0p5jT5o7b8phRabGK5Kg6X1UNOFQgSCpFyxaATUqPDwqIeja8Qs
ljLMnC8=
-----END CERTIFICATE-----
`
const testTlsPrivateKey = `-----BEGIN PRIVATE KEY-----
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
1XUed/WtsZtAhEtxUopFtBuwSGnNjjg=
-----END PRIVATE KEY-----
`
const testAuthTokenId = "my_test_user"
const testAuthToken = "GwBXoGG5c6NoUTLXnzMxw_uNiVa8PKobzx5EiuylMW0"

var rndStrAlphabet = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

type mockServer struct {
	ln         net.Listener
	listenPort int
}

type mockServerConfig struct {
	useTLS     bool
	enableAuth bool
}

func mockServerStop(ms *mockServer) {
	ms.ln.Close()
}

func mockServerStart(cfg mockServerConfig) *mockServer {
	var (
		ln  net.Listener
		err error
	)

	if cfg.useTLS {
		var cert tls.Certificate

		config := &tls.Config{}
		cert, err = tls.X509KeyPair([]byte(testTlsCertificate), []byte(testTlsPrivateKey))
		if err != nil {
			fatal("Failed to load test cert")
		}
		config.Certificates = make([]tls.Certificate, 1)
		config.Certificates = append(config.Certificates, cert)
		ln, err = tls.Listen("tcp", ":0", config)
	} else {
		ln, err = net.Listen("tcp", ":0")
	}
	if err != nil {
		fatal("Failed to start server listen socket: %s\n", err.Error())
	}
	fmt.Println("Mock TCP server listening on port:", ln.Addr().(*net.TCPAddr).Port)
	ms := &mockServer{
		ln:         ln,
		listenPort: ln.Addr().(*net.TCPAddr).Port,
	}

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return // socket is closed
			}
			go func() {
				data := make([]byte, 512)
				if cfg.enableAuth {
					expectedId := testAuthTokenId + "\n"
					rc, err := conn.Read(data)
					if err != nil {
						fatal("failed to read token id: ", err.Error())
					}
					if rc != len(expectedId) {
						fatal("unexpected token id len: ", expectedId)
					}
					actualId := string(data[:rc])
					if actualId != expectedId {
						fatal("unexpected token id: ", actualId)
					}

					_, err = conn.Write([]byte(randStr(512) + "\n"))
					if err != nil {
						fatal("failed to write challenge: ", err.Error())
					}

					// The rest is signature + data
				}

				for {
					_, err := conn.Read(data)
					if err != nil {
						if err != io.EOF {
							fatal("failed to read from connection: ", err.Error())
						}
						return
					}
				}
			}()
		}
	}()

	return ms
}

func randStr(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = rndStrAlphabet[rand.Intn(len(rndStrAlphabet))]
	}
	return string(b)
}

func TestProcessorInit(t *testing.T) {
	ms := mockServerStart(mockServerConfig{})
	defer mockServerStop(ms)
	questdbILPBindTo = fmt.Sprintf("127.0.0.1:%d", ms.listenPort)
	p := &processor{}
	p.Init(0, true, false)
	p.Close(true)

	p = &processor{}
	p.Init(1, true, false)
	p.Close(true)
}

func TestProcessorProcessBatch(t *testing.T) {
	bufPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
		},
	}
	f := &factory{}
	b := f.New().(*batch)
	pt := data.LoadedPoint{
		Data: []byte("tag1=tag1val,tag2=tag2val col1=0.0,col2=0.0 140\n"),
	}
	b.Append(pt)

	cases := []struct {
		doLoad bool
		cfg    mockServerConfig
	}{
		{
			doLoad: false,
			cfg: mockServerConfig{
				useTLS:     false,
				enableAuth: false,
			},
		},
		{
			doLoad: false,
			cfg: mockServerConfig{
				useTLS:     true,
				enableAuth: false,
			},
		},
		{
			doLoad: true,
			cfg: mockServerConfig{
				useTLS:     false,
				enableAuth: false,
			},
		},
		{
			doLoad: true,
			cfg: mockServerConfig{
				useTLS:     true,
				enableAuth: false,
			},
		},
		{
			doLoad: true,
			cfg: mockServerConfig{
				useTLS:     false,
				enableAuth: true,
			},
		},
		{
			doLoad: true,
			cfg: mockServerConfig{
				useTLS:     true,
				enableAuth: true,
			},
		},
	}

	for _, c := range cases {
		fatal = func(format string, args ...interface{}) {
			t.Errorf("fatal called for case %v unexpectedly\n", c)
			fmt.Printf(format, args...)
		}

		ms := mockServerStart(c.cfg)
		questdbILPBindTo = fmt.Sprintf("127.0.0.1:%d", ms.listenPort)

		useTLS = c.cfg.useTLS
		if c.cfg.enableAuth {
			authTokenId = testAuthTokenId
			authToken = testAuthToken
		} else {
			authTokenId = ""
			authToken = ""
		}

		p := &processor{}
		p.Init(0, true, true)
		mCnt, rCnt := p.ProcessBatch(b, c.doLoad)
		if mCnt != b.metrics {
			t.Errorf("process batch returned less metrics than batch: got %d want %d", mCnt, b.metrics)
		}
		if rCnt != uint64(b.rows) {
			t.Errorf("process batch returned less rows than batch: got %d want %d", rCnt, b.rows)
		}
		p.Close(true)
		mockServerStop(ms)
		time.Sleep(50 * time.Millisecond)
	}
}
