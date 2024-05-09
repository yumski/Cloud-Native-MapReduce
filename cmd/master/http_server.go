package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

func (m *Master) StartHTTPServer() {
	defer m.wg.Done()

	port := ":8080"

	server := &http.Server{
		Addr: port,
	}

	// http.HandleFunc("/live", LivenessHandler)
	http.HandleFunc("/mapreduce", m.MapReduceHandler)

	log.Printf("Starting http server")

	if err := server.ListenAndServe(); err != nil {
		log.Printf("HTTP server ListenAndServe: %v", err)
	}

}

func (m *Master) MapReduceHandler(w http.ResponseWriter, r *http.Request) {
	type Response struct {
		Files []string `json:"files"`
	}
	decoder := json.NewDecoder(r.Body)
	mrConfig := MapReduceSpec{}

	// parse the mapreduce config from json
	err := decoder.Decode(&mrConfig)
	if err != nil {
		respondWithError(w, 400, fmt.Sprintf("Error parsing json: %v", err))
		return
	}

	log.Printf("MapReduce job specification received: %v", mrConfig)

	files, err := m.MapReduce(mrConfig)

	if err != nil {
		respondWithError(w, 400, fmt.Sprintf("MapReduce unsuccessful: %v", err))
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
		Error string `json:"error"`
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
