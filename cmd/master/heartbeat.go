package main

import (
	"log"
	"sync"
	"time"

	pb "mapreduce/protos"
)

func (m *Master) StartHeartBeat() {
	defer m.wg.Done()

	wg := sync.WaitGroup{}

	for {
		for _, worker := range m.workerPool.workers {
			wg.Add(1)
			go m.HeartBeat(worker, &wg)
		}
		wg.Wait()
		time.Sleep(5 * time.Second)
	}
}

// perform the map request to the worker
func (m *Master) HeartBeat(worker *Worker, wg *sync.WaitGroup) {
	defer wg.Done()

	// connect to the worker pod
	// connect to gRPC server associated with the worker
	if err := worker.Connect(); err != nil {
		log.Println("Unable to connect to gRPC Servier: ", err)
	}
	defer worker.CloseConnection()

	checkRequest := pb.CheckRequest{
		Message: worker.addr,
	}

	log.Printf("Sending Heartbeat to Worker: %v", worker.addr)
	// send map request
	r, err := worker.client.CheckStatus(worker.ctx, &checkRequest)

	// if the request isn't successful, return the current shard to be added back into queue
	if err != nil {
		log.Printf("Error sending Heartbeat to Worker: %v", err)
		return
	}

	// get the worker status
	status := r.GetStatus()
	log.Printf("Worker %v Status: %v", worker.addr, status)

	if worker.status == FAIL {
		log.Printf("worker %v recovered from FAILED state", worker.addr)
		worker.status = IDLE
		m.workerPool.workerCh <- true
	}

}
