package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
)

type InputFileInfo struct {
	Name string
	Size int64
}

type MapReduceSpec struct {
	UserID         string
	InputFiles     []InputFileInfo
	NumMapTasks    uint
	NumReduceTasks uint
	MinMBSize      uint32
	MaxMBSize      uint32
	JobID          int32
}

func StartHTTPServer(shutDownChan chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	port := ":8080"

	server := &http.Server{
		Addr: port,
	}

	http.HandleFunc("/live", LivenessHandler)
	http.HandleFunc("/mapreduce", MapReduceHandler)

	go func() {
		// wait to receive shutdown signal
		<-shutDownChan
		// shut down http server
		if err := server.Shutdown(context.Background()); err != nil {
			// Error from closing listeners, or context timeout:
			log.Printf("HTTP server Shutdown: %v", err)
		}
	}()

	log.Printf("Starting http server")

	if err := server.ListenAndServe(); err != nil {
		log.Printf("HTTP server ListenAndServe: %v", err)
	}

}

func LivenessHandler(w http.ResponseWriter, r *http.Request) {
	type response struct {
		Status string
	}

	respondWithJson(w, 200, response{
		Status: "OK",
	})
}

func MapReduceHandler(w http.ResponseWriter, r *http.Request) {
	type Response struct {
		Files []string `json: files`
	}
	decoder := json.NewDecoder(r.Body)
	mrConfig := MapReduceSpec{}

	// parse the mapreduce config from json
	err := decoder.Decode(&mrConfig)
	if err != nil {
		respondWithError(w, 400, fmt.Sprintf("Error parsing json: %v", err))
		return
	}

	files := make([]string, 0)
	for _, file := range mrConfig.InputFiles {
		files = append(files, file.Name)
	}

	res := Response{
		Files: files,
	}

	respondWithJson(w, 200, res)
}

func respondWithError(w http.ResponseWriter, statusCode int, message string) {
	if statusCode > 499 {
		log.Printf("Responding with 5xx error: %s", message)
	}

	type errVal struct {
		Error string `json: "error"`
	}

	respondWithJson(w, statusCode, errVal{
		Error: message,
	})
}

func respondWithJson(w http.ResponseWriter, statusCode int, payload interface{}) {
	w.Header().Set("content-type", "application/json")

	dat, err := json.Marshal(payload)

	if err != nil {
		log.Println("Unable to marshal payload")
		w.WriteHeader(500)
		return
	}

	w.WriteHeader(statusCode)
	w.Write(dat)
}

func main() {
	http.HandleFunc("/mapreduce", MapReduceHandler)

	http.ListenAndServe(":8080", nil)
}
