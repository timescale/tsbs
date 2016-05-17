package main

import (
	"flag"
	"fmt"
	"log"
	"syscall"
	"os/exec"
	"strings"
	"time"
)

// Parse args:
var (
	benchmarkClientSshPrelude, benchmarkServerSshPrelude string
	benchmarkClientCmd, benchmarkServerCmd string
)
func init() {
	flag.StringVar(&benchmarkServerSshPrelude, "benchmark-server-ssh-prelude", "", "SSH prelude for server connection.")
	flag.StringVar(&benchmarkClientSshPrelude, "benchmark-client-ssh-prelude", "", "SSH prelude for client connection.")
	flag.StringVar(&benchmarkServerCmd, "benchmark-server-cmd", "", "Command to run on the benchmarking server.")
	flag.StringVar(&benchmarkClientCmd, "benchmark-client-cmd", "", "Command to run on the benchmarking client.")

	flag.Parse()
}
func main() {
	serverArgs := append(strings.Split(benchmarkServerSshPrelude, " "), benchmarkServerCmd)
	clientArgs := append(strings.Split(benchmarkClientSshPrelude, " "), benchmarkClientCmd)

	server := exec.Command("ssh", serverArgs...)
	client := exec.Command("ssh", clientArgs...)

	// start server, keep it running:
	err := server.Start()
	if err != nil {
		log.Fatal(err)
	}

	// wait for server to initialize:
	time.Sleep(2 * time.Second)


	// run client to completion, printing collected stdout:
	out, err := client.Output()
	fmt.Println(string(out))
	if err != nil {
		log.Fatal(err)
	}

	// signal the server to shut down:
	err = server.Process.Signal(syscall.SIGINT)
	if err != nil {
		log.Fatal(err)
	}

	// wait for server to shut down:
	_, err = server.Process.Wait()
	if err != nil {
		log.Fatal("wait error ", err)
	}
}
