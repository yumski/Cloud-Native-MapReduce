package main

import (
	"context"
	"fmt"
	"log"
	pb "mapreduce/protos"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type InputFileInfo struct {
	Name string
	Size int64
}

type MapReduceSpec struct {
	UserID         string
	JobID          int32
	ContainerName  string
	InputFiles     []InputFileInfo
	NumMapTasks    uint
	NumReduceTasks uint
	MapperFunc     string
	ReducerFunc    string
}

// ShardFileInfo represents information about a file shard
type ShardFileInfo struct {
	Name  string
	Start int64
	End   int64
}

// FileShard represents a list of shard files in one partition
type FileShard struct {
	ShardFiles []ShardFileInfo
	ShardID    int
}

type Worker struct {
	addr   string
	status string
	ctx    context.Context
	client pb.MapReduceClient
	cancel context.CancelFunc
	conn   *grpc.ClientConn
}

const (
	IDLE    = "idle"
	SERVING = "serving"
	FAIL    = "fail"
)

// connecting worker to the associated gRPC Server
func (w *Worker) Connect() error {
	conn, err := grpc.Dial(
		w.addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)

	if err != nil {
		log.Printf("did not connect: %v", addr)
		return err
	}

	w.conn = conn

	log.Println("Connected to Worker: ", w.addr)

	w.client = pb.NewMapReduceClient(conn)

	// w.ctx, w.cancel = context.WithTimeout(context.Background(), 600*time.Second)

	clientDeadline := time.Now().Add(600 * time.Second)
	w.ctx, w.cancel = context.WithDeadline(context.Background(), clientDeadline)

	return nil
}

// close connection
func (w *Worker) CloseConnection() {
	w.conn.Close()
	w.cancel()
}

// Data structure to keep track of the workers
type WorkerPool struct {
	workers  []*Worker
	mu       sync.Mutex
	workerCh chan bool
}

func (wp *WorkerPool) CreateWorkerPool() {
	// perform DNS request to headless service to get worker ips
	addresses, err := net.LookupHost(fmt.Sprintf("%s.map-reduce.svc.cluster.local", serviceName))
	if err != nil {
		log.Printf("Error resolving DNS: %v\n", err)
		return
	}

	wp.InitChannel(len(addresses))

	// create the workers that are mapped to the return addresses
	log.Println("Created worker pool with following workers:")
	for _, address := range addresses {
		wp.AddWorker(&Worker{
			addr:   address + ":50051",
			status: IDLE,
		})
		log.Printf("Created Worker with addr: %v", address)
	}

}

// initialize the channel, which is used to wait when there are no workers available
func (wp *WorkerPool) InitChannel(bufSize int) {
	wp.workerCh = make(chan bool, bufSize)
}

// add a worker to the pool
func (wp *WorkerPool) AddWorker(worker *Worker) {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	wp.workers = append(wp.workers, worker)
	wp.workerCh <- true
}

// return a worker that's available
func (wp *WorkerPool) GetWorker() *Worker {

	// wait available worker
	<-wp.workerCh

	wp.mu.Lock()
	defer wp.mu.Unlock()

	// return the first available worker that is available
	for _, worker := range wp.workers {
		if worker.status == IDLE {
			worker.status = SERVING
			return worker
		}
	}

	return nil
}

func (wp *WorkerPool) ReturnWorker(worker *Worker) {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	if worker.status == SERVING {
		worker.status = IDLE
		wp.workerCh <- true
	}
}

func (m *Master) CreateWorkerPool() {
	m.workerPool = WorkerPool{}
	m.workerPool.CreateWorkerPool()
}
