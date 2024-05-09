package main

import (
	"context"
	"log"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

func (m *Master) InitEtcdClient() {

	log.Println("Connecting to ETCD address: ", etcdAddr)

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}, DialTimeout: 5 * time.Second})
	if err != nil {
		log.Fatalf("error creating clientv3: %v", err)
	}
	m.etcdcli = cli
}

func (m *Master) StartElection() {
	defer m.wg.Done()

	s, err := concurrency.NewSession(m.etcdcli)
	if err != nil {
		log.Fatalf("error creating new concurrency session: %v", err)
	}
	defer s.Close()

	log.Println("Starting election")

	m.e = concurrency.NewElection(s, "/leader-election/")
	m.electionCtx = context.Background()

	if err := m.e.Campaign(m.electionCtx, "e"); err != nil {
		log.Fatalf("error in campaign: %v", err)
	}

	log.Println("Elected as leader")

	// closing the channel to notify main thread to start http server
	close(m.readyChan)

	// Keep running as leader until the context is canceled
	for {
		select {
		case <-m.electionCtx.Done():
			log.Println("Context canceled, stepping down as leader")
			return
		}
	}

}

func (m *Master) ResignElection() error {
	if err := m.e.Resign(m.electionCtx); err != nil {
		return err
	}
	return nil
}
