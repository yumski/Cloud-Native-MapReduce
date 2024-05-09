package main

import (
	"fmt"
	"log"
	"math"
	"os"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"time"

	"mapreduce/cmd/azure"
	pb "mapreduce/protos"
)

var (
	addr = os.Getenv("INTERNAL_SERVICE_DNS") + ":50051"
	// podName     = os.Getenv("MY_POD_NAME")
	serviceName = os.Getenv("INTERNAL_SERVICE_DNS")
	failCount   = os.Getenv("FAIL")
	taskCount   int
)

func (m *Master) MapReduce(mrSpec MapReduceSpec) ([]string, error) {

	// print mrSpec
	log.Println("MapReduce Spec: ", mrSpec)

	taskCount = 0
	fail, err := strconv.Atoi(failCount)
	if err != nil {
		log.Println("Error converting fail count to int: ", err)
	}
	log.Println("Fail Count: ", fail)

	// save default checkpoint
	_, exists := m.checkPoint.JobDict[mrSpec.JobID]
	if !exists {
		m.checkPoint.JobDict = make(map[int32]JobStatus)
		m.checkPoint.JobDict[mrSpec.JobID] = JobStatus{mrSpec, make([]bool, mrSpec.NumMapTasks), make([]bool, mrSpec.NumReduceTasks)}
		m.SaveCheckpoint()
	}

	azureHelper := azure.AzureBlobHelper{}
	err = azureHelper.Init()
	if err != nil {
		log.Fatal("Error connecting to azure: ", err.Error())
	}

	// m.workerPool.InitChannel(int(mrSpec.NumMapTasks))

	// perform map tasks
	intermFiles, err := m.Map(azureHelper, &m.workerPool, mrSpec, fail)
	if err != nil {
		log.Fatal("Error mapping: ", err.Error())
	}

	log.Println("Map phase done, intermediate files: ", intermFiles)

	// m.workerPool.InitChannel(int(mrSpec.NumReduceTasks))

	// create and initialize the reducer file data structure
	groupedIntermFiles := make([][]string, mrSpec.NumReduceTasks)
	for i := range groupedIntermFiles {
		groupedIntermFiles[i] = make([]string, 0)
	}

	// append each intermediate file to their corresponding index
	for _, file := range intermFiles {
		// index, err := strconv.Atoi(string(file[len(file)-1]))
		// if err != nil {
		// 	log.Println("Error getting interm file index: ", err)
		// }
		re := regexp.MustCompile(`\d+$`)
		indexStr := re.FindString(file)
		index, err := strconv.Atoi(indexStr)
		if err != nil {
			log.Println("Error getting interm file index: ", err)
		}
		groupedIntermFiles[index] = append(groupedIntermFiles[index], file)
	}

	outputFiles := []string{}

	// perform reduce tasks
	outputFile, err := m.Reduce(azureHelper, &m.workerPool, mrSpec, groupedIntermFiles, fail)
	if err != nil {
		log.Fatal("Error reducing: ", err.Error())
	}
	outputFiles = append(outputFiles, outputFile...)
	log.Println("Reduce phase done, Output Files: ", outputFiles)

	// remove checkpoint before return result to client
	delete(m.checkPoint.JobDict, mrSpec.JobID)
	m.SaveCheckpoint()

	return outputFiles, nil
}

// create map requests according to map specs and shard list
func CreateMapRequests(mrSpec MapReduceSpec, fileShards []FileShard) []pb.MapRequest {

	MapRequests := make([]pb.MapRequest, 0)

	for i := range fileShards {
		var curMapReq pb.MapRequest
		for _, shardfileinfo := range fileShards[i].ShardFiles {
			shardfile := new(pb.ShardInfo)
			shardfile.FileName = shardfileinfo.Name
			shardfile.Start = int32(shardfileinfo.Start)
			shardfile.End = int32(shardfileinfo.End)
			curMapReq.ShardFiles = append(curMapReq.ShardFiles, shardfile)
		}
		curMapReq.UserId = mrSpec.UserID
		curMapReq.ShardId = int32(i)
		curMapReq.NumReduceTasks = int32(mrSpec.NumReduceTasks)
		MapRequests = append(MapRequests, pb.MapRequest{
			UserId:         mrSpec.UserID,
			ContainerName:  mrSpec.ContainerName,
			ShardFiles:     curMapReq.ShardFiles,
			ShardId:        int32(i),
			NumMapTasks:    int32(mrSpec.NumMapTasks),
			NumReduceTasks: int32(mrSpec.NumReduceTasks),
			JobId:          mrSpec.JobID,
			MapperFunc:     mrSpec.MapperFunc,
		})
	}

	return MapRequests
}

// create map requests according to map specs and shard list
func CreateReduceRequests(mrSpec MapReduceSpec, intermFiles [][]string) []pb.ReduceRequest {

	reduceRequest := make([]pb.ReduceRequest, 0)

	for i := range intermFiles {
		reduceRequest = append(reduceRequest, pb.ReduceRequest{
			UserId:            mrSpec.UserID,
			ReducerId:         int32(i),
			JobId:             mrSpec.JobID,
			IntermediateFiles: intermFiles[i],
			ReducerFunc:       mrSpec.ReducerFunc,
		})
	}
	return reduceRequest
}

// estimate sharding of the input files
func ShardFiles(mrSpec MapReduceSpec) []FileShard {

	// sort input files by size
	sort.Slice(mrSpec.InputFiles, func(i, j int) bool {
		return mrSpec.InputFiles[i].Size < mrSpec.InputFiles[j].Size
	})

	// calculate total file size
	totalSize := int64(0)
	for _, fileInfo := range mrSpec.InputFiles {
		totalSize += fileInfo.Size
	}

	// calculate shard size
	shardSize := int64(math.Ceil(float64(totalSize) / float64(mrSpec.NumMapTasks)))

	var fileShards []FileShard
	var curShard FileShard
	start, end, remains := int64(0), int64(0), shardSize

	for _, currentFile := range mrSpec.InputFiles {
		filename := currentFile.Name
		bytesLeft := currentFile.Size

		// If current file size is less than remaining shard size, add current file to the shard file list
		if bytesLeft < remains {
			end = bytesLeft
			remains -= bytesLeft
			curShard.ShardFiles = append(curShard.ShardFiles, ShardFileInfo{Name: filename, Start: start, End: end})
		} else {
			// continously build shards from the file until the remainder is less than the shard size
			for bytesLeft >= remains {
				// add the remainder of bytes to fill the shard to the end pos
				end += remains
				// add the shardfile to the current shard
				curShard.ShardFiles = append(curShard.ShardFiles, ShardFileInfo{Name: filename, Start: start, End: end})
				// add the current shard to the file shard
				fileShards = append(fileShards, FileShard{
					ShardFiles: curShard.ShardFiles,
				})
				// set the starting position to thecurrent end
				start = end
				// subtract the remains from the number of bytes left in file
				bytesLeft -= remains
				// reset the remains variable
				remains = shardSize
				curShard = FileShard{}
			}
			// if there are still bytes left in the file add the remainder into the shard
			if bytesLeft > 0 {
				end += bytesLeft
				curShard.ShardFiles = append(curShard.ShardFiles, ShardFileInfo{Name: filename, Start: start, End: end})
				remains -= bytesLeft
			}
		}

		start, end = 0, 0
	}

	if remains != shardSize {
		fileShards = append(fileShards, FileShard{
			ShardFiles: curShard.ShardFiles,
		})
	}
	return fileShards
}

// perform the map phase of the mapreduce job
func (m *Master) Map(azhelper azure.AzureBlobHelper, workerPool *WorkerPool, mrSpec MapReduceSpec, fail int) ([]string, error) {

	// get the shardlist
	shardList := ShardFiles(mrSpec)
	if len(shardList) == 0 {
		log.Println("No shards to process")
		return []string{}, nil
	}
	log.Printf("Container Name: %v", mrSpec.ContainerName)
	log.Printf("Shard list: %v : %v", len(shardList), shardList)

	// create map requests
	mapRequests := CreateMapRequests(mrSpec, shardList)

	// synchronization variables for the mappers
	var wg sync.WaitGroup
	var mu sync.Mutex
	resultChan := make(chan []string, mrSpec.NumMapTasks)
	mapTasksRemaining := len(mapRequests)
	intermFiles := []string{}

	log.Println("Starting Map Tasks: ", mapTasksRemaining)

	// keep repeating until all maptasks are done
	for mapTasksRemaining > 0 {

		// pop first item off the shard list and decrement tasks remaining
		mu.Lock()
		request := &mapRequests[0]
		mapRequests = mapRequests[1:]
		mapTasksRemaining--
		mu.Unlock()

		if m.checkPoint.JobDict[mrSpec.JobID].MapRequestStatus[request.ShardId] {
			log.Printf("Map Request already completed: %v", request.ShardId)
			// construct intermediate files
			finishedIntermFiles := []string{}
			for i := range request.NumReduceTasks {
				finishedIntermFiles = append(finishedIntermFiles, fmt.Sprintf("%v_j%d_m%d_r%d", mrSpec.UserID, mrSpec.JobID, request.ShardId, i))
			}
			resultChan <- finishedIntermFiles
		} else {

			wg.Add(1)
			go func() {
				defer wg.Done()

				for {
					worker := m.workerPool.GetWorker()
					if worker == nil {
						log.Println("No workers available, waiting for worker to be added back to pool")
					} else {
						request.WorkerId = worker.addr
						returnFiles, err := DoMapRequest(worker, request)

						if err != nil {
							worker.status = FAIL
						}
						m.workerPool.ReturnWorker(worker)

						if err != nil {
							log.Printf("map request %d failed with \"%v\" on worker %v, retry %d", request.ShardId, err, worker.addr, 0)
							time.Sleep(10 * time.Second)
						} else {
							log.Printf("Intermediate Files: %v", returnFiles)
							resultChan <- returnFiles
							break
						}
					}
				}

				log.Println("Map Request Completed: ", request.ShardId)
				mu.Lock()
				taskCount++
				m.checkPoint.JobDict[mrSpec.JobID].MapRequestStatus[request.ShardId] = true
				m.SaveCheckpoint()
				mu.Unlock()

				log.Printf("Finished Task Count: %v, fail count %v", taskCount, fail)
				if taskCount+1 == fail {
					log.Println("fail count reached!")
					m.ResignElection()
					os.Exit(0)
				}
			}()
		}
		log.Println("Map tasks remaining: ", mapTasksRemaining)
	}
	log.Println("All Map Tasks are submitted.")

	go func() {
		wg.Wait()
		close(resultChan)
		log.Println("All Map Tasks are finished")
	}()

	log.Println("read the results from the maptasks")
	// read the results from the maptasks
	for files := range resultChan {
		intermFiles = append(intermFiles, files...)
	}

	// print the intermediate files
	// log.Printf("Intermediate files: %v", intermFiles)

	return intermFiles, nil
}

// perform the map phase of the mapreduce job
func (m *Master) Reduce(azhelper azure.AzureBlobHelper, workerPool *WorkerPool, mrSpec MapReduceSpec, intermFiles [][]string, fail int) ([]string, error) {

	// log.Println("Intermediate Files: ", intermFiles)

	// create map requests
	reduceRequests := CreateReduceRequests(mrSpec, intermFiles)

	// synchronization variables for the mappers
	var wg sync.WaitGroup
	var mu sync.Mutex
	resultChan := make(chan string, mrSpec.NumReduceTasks)
	reduceTasksRemaining := len(reduceRequests)
	outputFiles := []string{}

	log.Println("Starting Reduce Tasks: ", reduceTasksRemaining)

	// keep repeating until all maptasks are done
	for reduceTasksRemaining > 0 {

		// pop first item off the shard list and decrement tasks remaining
		mu.Lock()
		request := &reduceRequests[0]
		reduceRequests = reduceRequests[1:]
		reduceTasksRemaining--
		mu.Unlock()

		if m.checkPoint.JobDict[mrSpec.JobID].ReduceRequestStatus[request.ReducerId] {
			log.Printf("Reduce Request already completed: %v", request.ReducerId)
			resultChan <- fmt.Sprintf("%v_j%d_r%d", mrSpec.UserID, mrSpec.JobID, request.ReducerId)
		} else {

			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					worker := m.workerPool.GetWorker()
					if worker == nil {
						log.Println("No workers available, waiting for worker to be added back to pool")
					} else {
						request.WorkerId = worker.addr
						returnFile, err := DoReduceRequest(worker, request)

						if err != nil {
							worker.status = FAIL
						}
						m.workerPool.ReturnWorker(worker)

						if err != nil {
							log.Printf("reduce request %d failed with \"%v\" on worker %v, retry %d", request.ReducerId, err, worker.addr, 0)
							time.Sleep(10 * time.Second)
						} else {
							resultChan <- returnFile
							break
						}
					}
				}

				log.Println("Reduce Request Completed: ", request.ReducerId)
				mu.Lock()
				taskCount++
				m.checkPoint.JobDict[mrSpec.JobID].ReduceRequestStatus[request.ReducerId] = true
				m.SaveCheckpoint()
				mu.Unlock()

				log.Printf("Finished Task Count: %v, fail count %v", taskCount, fail)
				if taskCount+1 == fail {
					log.Println("fail count reached!")
					m.ResignElection()
					os.Exit(0)
				}
			}()
		}
		log.Println("Reduce Tasks remaining: ", reduceTasksRemaining)
	}
	log.Println("All Reduce Tasks are submitted.")

	go func() {
		wg.Wait()
		close(resultChan)
		log.Println("All Reduce Tasks are finished")
	}()

	// read the results from the maptasks
	for file := range resultChan {
		outputFiles = append(outputFiles, file)
	}

	// print the intermediate files
	log.Printf("Output Files: %v", outputFiles)
	return outputFiles, nil

}

// perform the map request to the worker
func DoMapRequest(worker *Worker, mapRequest *pb.MapRequest) ([]string, error) {
	// connect to gRPC server associated with the worker
	if err := worker.Connect(); err != nil {
		log.Println("Unable to connect to gRPC Servier: ", err)
	}
	defer worker.CloseConnection()

	// send map request
	r, err := worker.client.Map(worker.ctx, mapRequest)

	// if the request isn't successful, return the current shard to be added back into queue
	if err != nil {
		return []string{}, err
	}

	// return the interm files
	intermFiles := r.GetIntermediateFiles()
	// log.Println(intermFiles)
	return intermFiles, nil
}

// perform the map request to the worker
func DoReduceRequest(worker *Worker, reduceRequest *pb.ReduceRequest) (string, error) {

	// connect to gRPC server associated with the worker
	if err := worker.Connect(); err != nil {
		log.Println("Unable to connect to gRPC Servier: ", err)
	}
	defer worker.CloseConnection()

	// send map request
	r, err := worker.client.Reduce(worker.ctx, reduceRequest)

	// if the request isn't successful, return the current shard to be added back into queue
	if err != nil {
		return "", err
	}

	// return the final files
	outputFile := r.GetOutputFile()
	// log.Println(outputFile)
	return outputFile, nil
}
