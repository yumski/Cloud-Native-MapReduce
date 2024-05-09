package main

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

func main() {
	log.Printf("Pod Name: %v", podName)
	log.Printf("Pod IP: %v", podIp)

	// listen for sigterm signals
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGTERM)

	// create channel to notify the go routines to shutdown
	shutDownChan := make(chan struct{})

	// Create waitgroup to sync goroutines
	var wg sync.WaitGroup
	wg.Add(1)

	go StartgRPCServer(shutDownChan, &wg)

	// wait for the sigterm signal
	<-signalChan

	// close the shutDownChan channel to notify the other goroutines to stop
	close(shutDownChan)

	// wait for the other goroutines to finish
	wg.Wait()

}
