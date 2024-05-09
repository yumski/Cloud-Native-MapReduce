package main

import (
	"context"
	"log"
	"net/http"
	"sync"
)

func HealthCheckHandler(shutDownChan chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	var server http.Server

	http.HandleFunc("/live", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	go func() {
		// wait to receive shutdown signal
		<-shutDownChan
		// shut down http server
		if err := server.Shutdown(context.Background()); err != nil {
			// Error from closing listeners, or context timeout:
			log.Printf("HTTP server Shutdown: %v", err)
		}
	}()

	if err := server.ListenAndServe(); err != nil {
		log.Printf("HTTP server ListenAndServe: %v", err)
	}
}
