package main

import (
	"bufio"
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"math/big"
	"net"

	"github.com/timescale/tsbs/pkg/targets"
)

type processor struct {
	ilpConn net.Conn
}

func (p *processor) Init(numWorker int, doLoad, _ bool) {
	if !doLoad {
		return
	}

	var (
		d    net.Dialer
		key  *ecdsa.PrivateKey
		conn net.Conn
		err  error
	)

	if authTokenId != "" && authToken != "" {
		keyRaw, err := base64.RawURLEncoding.DecodeString(authToken)
		if err != nil {
			fatal("failed to decode auth key: %v", err)
		}
		key = new(ecdsa.PrivateKey)
		key.PublicKey.Curve = elliptic.P256()
		key.PublicKey.X, key.PublicKey.Y = key.PublicKey.Curve.ScalarBaseMult(keyRaw)
		key.D = new(big.Int).SetBytes(keyRaw)
	}

	ctx := context.Background()
	if useTLS {
		config := &tls.Config{}
		config.InsecureSkipVerify = true
		conn, err = tls.DialWithDialer(&d, "tcp", questdbILPBindTo, config)
	} else {
		conn, err = d.DialContext(ctx, "tcp", questdbILPBindTo)
	}
	if err != nil {
		fatal("Failed connect to %s: %s\n", questdbILPBindTo, err.Error())
	}

	if key != nil {
		_, err = conn.Write([]byte(authTokenId + "\n"))
		if err != nil {
			fatal("failed to write key id: %v", err)
		}

		reader := bufio.NewReader(conn)
		raw, err := reader.ReadBytes('\n')
		if len(raw) < 2 {
			fatal("empty challenge response from server: %v", err)
		}
		// Remove the `\n` in the last position.
		raw = raw[:len(raw)-1]
		if err != nil {
			fatal("failed to read challenge response from server: %v", err)
		}

		// Hash the challenge with sha256.
		hash := crypto.SHA256.New()
		hash.Write(raw)
		hashed := hash.Sum(nil)

		stdSig, err := ecdsa.SignASN1(rand.Reader, key, hashed)
		if err != nil {
			fatal("failed to sign challenge using auth key: %v", err)
		}
		_, err = conn.Write([]byte(base64.StdEncoding.EncodeToString(stdSig) + "\n"))
		if err != nil {
			fatal("failed to write signed challenge: %v", err)
		}
	}

	p.ilpConn = conn
}

func (p *processor) Close(doLoad bool) {
	if doLoad {
		p.ilpConn.Close()
	}
}

func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (uint64, uint64) {
	batch := b.(*batch)

	// Write the batch: try until backoff is not needed.
	if doLoad {
		_, err := p.ilpConn.Write(batch.buf.Bytes())
		if err != nil {
			fatal("Error writing: %s\n", err.Error())
		}
	}

	metricCnt := batch.metrics
	rowCnt := batch.rows

	// Return the batch buffer to the pool.
	batch.buf.Reset()
	bufPool.Put(batch.buf)
	return metricCnt, uint64(rowCnt)
}
