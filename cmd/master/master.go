package main

import (
	"context"
	"os"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type Master struct {
	// synchronization for the master
	wg        sync.WaitGroup
	readyChan chan struct{}

	// election variables
	etcdcli     *clientv3.Client
	e           *concurrency.Election
	electionCtx context.Context

	workerPool WorkerPool

	checkPoint CheckPoint
}

var (
	etcdAddr = os.Getenv("ETCD_DNS") + ":2379"
)

func (m *Master) Start() {
	m.readyChan = make(chan struct{})
	m.InitEtcdClient()
	defer m.etcdcli.Close()

	m.wg.Add(3)
	go m.StartElection()

	<-m.readyChan

	m.LoadCheckpoint()

	m.CreateWorkerPool()

	go m.StartHeartBeat()

	go m.StartHTTPServer()

	m.wg.Wait()
}

func main() {

	master := Master{}

	master.Start()

}
