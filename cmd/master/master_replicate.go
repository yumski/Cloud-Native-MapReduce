package main

import (
	"context"
	"encoding/json"
	"log"
)

// unfinished job status
type JobStatus struct {
	MRSpec              MapReduceSpec
	MapRequestStatus    []bool
	ReduceRequestStatus []bool
}

type CheckPoint struct {
	JobDict map[int32]JobStatus
}

func (m *Master) SaveCheckpoint() {
	// save the checkpoint
	log.Printf("Saving checkpoint: %v", m.checkPoint)
	m.saveValue("checkpoint", m.checkPoint)

}

func (m *Master) LoadCheckpoint() {

	// DEBUG: NEED UNCOMMENT
	// checkPoint := CheckPoint{}

	checkPoint, err := m.readValue("checkpoint")
	if err != nil {
		log.Fatalf("error reading checkpoint: %v", err)
	}

	log.Printf("Loaded checkpoint, %v", checkPoint)
	m.checkPoint = checkPoint
}

func (m *Master) saveValue(key string, value CheckPoint) error {
	jsonValue, err := json.Marshal(value)
	if err != nil {
		log.Fatalf("error marshalling value: %v", err)
	}

	_, err = m.etcdcli.Put(context.Background(), key, string(jsonValue))
	if err != nil {
		log.Fatalf("error putting value: %v", err)
	}

	return nil
}

func (m *Master) readValue(key string) (CheckPoint, error) {
	resp, err := m.etcdcli.Get(context.Background(), key)
	if err != nil {
		log.Fatalf("error getting value: %v", err)
		return CheckPoint{}, err
	}

	var data CheckPoint
	for _, ev := range resp.Kvs {
		err := json.Unmarshal(ev.Value, &data)
		if err != nil {
			log.Fatalf("error unmarshalling value: %v", err)
			return CheckPoint{}, err
		}
	}

	return data, nil
}
