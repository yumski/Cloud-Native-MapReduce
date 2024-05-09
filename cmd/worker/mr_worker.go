package main

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"

	"mapreduce/cmd/azure"
	pb "mapreduce/protos"

	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedMapReduceServer
	workerStatus map[string]pb.CheckResponse_ServingStatus
}

var (
	port             = flag.Int("port", 50051, "Enter server port")
	podIp            = os.Getenv("MY_POD_IP")
	podName          = os.Getenv("MY_POD_NAME")
	fail             = os.Getenv("FAIL")
	azBlobHelper     azure.AzureBlobHelper
	workingContainer = "map-reduce-team17"
	taskCount        = 0
	failCount        = 0
	mu               sync.Mutex
)

func StartgRPCServer(shutdownChan chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", *port)
	}

	azBlobHelper = azure.AzureBlobHelper{}
	azBlobHelper.Init()

	s := grpc.NewServer()
	pb.RegisterMapReduceServer(s, &server{workerStatus: make(map[string]pb.CheckResponse_ServingStatus)})

	go func() {
		<-shutdownChan
		s.GracefulStop()
	}()

	failInt, err := strconv.Atoi(fail)
	if err != nil {
		log.Println("Error converting fail count to int: ", err)
	}
	failCount = failInt

	log.Printf("Server listening on port: %v", *port)

	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func (s *server) CheckStatus(ctx context.Context, in *pb.CheckRequest) (*pb.CheckResponse, error) {

	if _, ok := s.workerStatus[in.Message]; !ok {
		s.workerStatus[in.Message] = pb.CheckResponse_IDLE
	}

	status := s.workerStatus[in.Message]
	s.workerStatus[in.Message] = pb.CheckResponse_IDLE

	log.Printf("CheckStatus request received, returning status: %v", status)

	return &pb.CheckResponse{
		Status: status,
	}, nil
}

func (s *server) Map(ctx context.Context, in *pb.MapRequest) (*pb.MapResponse, error) {
	log.Println()
	log.Printf("Map task received: %v", in.GetShardId())
	// log.Printf("UserID: %v", in.GetUserId())
	log.Printf("ShardID: %v", in.GetShardId())
	log.Printf("NumMapTasks: %v", in.GetNumMapTasks())
	log.Printf("NumReduceTasks: %v", in.GetNumReduceTasks())
	log.Printf("ContainerName: %v", in.GetContainerName())
	log.Printf("FileList: %v", in.GetShardFiles())

	if ctx.Err() == context.Canceled {
		log.Println("Client is canceled")
		return nil, fmt.Errorf("client is canceled")
	}

	mu.Lock()
	log.Printf("Finished Task Count: %v, fail count %v", taskCount, failCount)
	if taskCount+1 == failCount {
		log.Println("fail count reached!")
		mu.Unlock()
		os.Exit(0)
	}
	mu.Unlock()

	s.workerStatus[in.WorkerId] = pb.CheckResponse_MAPPING
	// get shard files
	fileShards := in.GetShardFiles()

	// create intermediate files
	blobname := fmt.Sprintf("%v_j%d_m%d", in.UserId, in.JobId, in.ShardId)
	log.Printf("Creating intermediate file: %s", blobname)
	intermediateFiles, err := azBlobHelper.CreateIntermediateFiles(in.ContainerName, blobname, in.GetNumReduceTasks(), fileShards)
	if err != nil {
		log.Printf("Error creating intermediate file: %v", err)
		return nil, err
	}

	mu.Lock()
	taskCount++
	mu.Unlock()
	log.Printf("Finished Task count: %v", taskCount)

	return &pb.MapResponse{
		IntermediateFiles: intermediateFiles,
	}, nil

}

func (s *server) Reduce(ctx context.Context, in *pb.ReduceRequest) (*pb.ReduceResponse, error) {
	log.Println()
	log.Printf("Reduce task received: %v", in.GetReducerId())
	// log.Printf("UserID: %v", in.GetUserId())
	log.Printf("ReducerID: %v", in.GetReducerId())
	log.Printf("IntermediateFiles: %v", in.GetIntermediateFiles())

	if ctx.Err() == context.Canceled {
		log.Println("Client is canceled")
		return nil, fmt.Errorf("client is canceled")
	}

	mu.Lock()
	log.Printf("Finished Task Count: %v, fail count %v", taskCount, failCount)
	if taskCount+1 == failCount {
		log.Println("fail count reached!")
		mu.Unlock()
		os.Exit(0)
	}
	mu.Unlock()

	s.workerStatus[in.WorkerId] = pb.CheckResponse_REDUCING

	// create output file
	intermediateFiles := in.GetIntermediateFiles()
	reducedFile := fmt.Sprintf("%v_j%d_r%d", in.UserId, in.JobId, in.ReducerId)
	// create synchronization variables
	var mu sync.Mutex
	var wg sync.WaitGroup
	// data structure to hold the map data
	reducerData := make([]string, 0)

	// read the data from all the intermediate files
	for _, file := range intermediateFiles {
		wg.Add(1)
		go func() {
			defer wg.Done()
			data, err := azBlobHelper.DownloadBlobData(workingContainer, file)
			if err != nil {
				log.Printf("Error downloading blob: %v", err)
				return
			}

			byteReader := bytes.NewReader(data)
			reader := bufio.NewReader(byteReader)

			for {
				line, err := reader.ReadBytes('\n')
				if err == io.EOF {
					break
				}
				// synchronized writes to the slice
				mu.Lock()
				reducerData = append(reducerData, string(line))
				mu.Unlock()
			}
		}()
	}

	// wait for all the goroutines to finish reading the intermediate files
	wg.Wait()
	log.Println("All intermediate files have been read by reducer")

	// sort reduced file data
	sort.Strings(reducerData)
	log.Println("Finished sorting the data")

	// convert the string slice to a string for the reducer function
	var reducerString strings.Builder
	for _, s := range reducerData {
		reducerString.Write([]byte(s))
	}
	reducerData = nil

	// get the reducer that was supplied by the client
	reducerName := in.GetReducerFunc()
	log.Println("Reducer: ", reducerName)
	customReducer, err := azBlobHelper.DownloadBlobData(workingContainer, reducerName)
	if err != nil {
		log.Printf("Error download reducer: %v", err)
		return nil, err
	}

	// execute the reducer function and stdinpipe
	cmd := exec.Command("python3", "-c", string(customReducer))
	stdin, err := cmd.StdinPipe()
	if err != nil {
		log.Println("Stdin pipe error: ", err)
		return nil, err
	}

	// write the data to the reducer to process
	go func() {
		defer stdin.Close()
		if _, err := stdin.Write([]byte(reducerString.String())); err != nil {
			log.Println("Error writing: ", err)
			return
		}
	}()

	// read the output from the reducer function
	out, err := cmd.Output()
	if err != nil {
		log.Println("Reading output error: ", err)
		return nil, err
	}

	// upload the blob to azure
	err = azBlobHelper.UploadBlobData(workingContainer, reducedFile, string(out))
	if err != nil {
		log.Println(err)
	}

	mu.Lock()
	taskCount++
	mu.Unlock()
	log.Printf("Finished Task count: %v", taskCount)

	return &pb.ReduceResponse{
		OutputFile: reducedFile,
	}, nil
}
