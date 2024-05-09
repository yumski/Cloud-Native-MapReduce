package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"log"
	"mapreduce/cmd/azure"
	"math/rand/v2"
	"net/http"
	"os"
	"time"
)

type FileSpec struct {
	FileName string `json:"filename"`
}

type InputFileInfo struct {
	Name string
	Size int64
}

type InputOptions struct {
	UserID         string
	ContainerName  string
	InputFilesDir  string
	OutputFilesDir string
	NumMapTasks    uint
	NumReduceTasks uint
	Url            string
	MapperFunc     string
	ReducerFunc    string
}

type MapReduceSpec struct {
	JobID          int32
	UserID         string
	ContainerName  string
	InputFiles     []InputFileInfo
	NumMapTasks    uint
	NumReduceTasks uint
	MapperFunc     string
	ReducerFunc    string
}

type UploadSpec struct {
	UploadFilesDir string
	ContainerName  string
}

var (
	uploadSpec       = flag.String("upload", "", "Enter the upload spec file")
	mrSpec           = flag.String("mrspec", "", "Enter the mapreduce spec file")
	azBlobHelper     azure.AzureBlobHelper
	workingContainer = "map-reduce-team17"
)

func main() {
	// Parse the flags
	flag.Parse()
	if *uploadSpec == "" && *mrSpec == "" {
		log.Fatalf("Please provide either -upload or -mrspec flag")
	}

	// initialize the azure blob helper
	azBlobHelper = azure.AzureBlobHelper{}
	azBlobHelper.Init()

	// upload files to azure blob storage
	if *uploadSpec != "" {
		uploadFiles(*uploadSpec)
	}

	// run mapreduce job
	if *mrSpec != "" {
		runMapReduce(*mrSpec)
	}

}

func uploadFiles(uploadOption string) {
	// Read the mapreduce spec
	uploadData, err := os.ReadFile(uploadOption)
	if err != nil {
		log.Fatalf("Error reading mapreduce spec: %v", err)
	}

	uploadOptions := UploadSpec{}
	err = json.Unmarshal(uploadData, &uploadOptions)
	if err != nil {
		log.Fatalf("Error unmarshalling mapreduce spec: %v", err)
	}

	uploadDir := uploadOptions.UploadFilesDir
	containerName := uploadOptions.ContainerName

	// iterate upload directory and upload files
	dir, err := os.ReadDir(uploadDir)
	if err != nil {
		log.Fatalf("Error reading upload directory: %v", err)
	}

	for _, file := range dir {
		if !file.IsDir() {
			log.Printf("Uploading file: %s", file.Name())
			err := azBlobHelper.UploadFile(containerName, uploadDir+"/"+file.Name())
			if err != nil {
				log.Fatalf("Error uploading file: %v", err)
			}
		}
	}
}

func runMapReduce(inputOption string) {
	type outputFiles struct {
		Files []string `json:"files"`
	}

	http.DefaultClient.Timeout = 0

	// Read the mapreduce spec
	inputOptionData, err := os.ReadFile(inputOption)
	if err != nil {
		log.Fatalf("Error reading mapreduce spec: %v", err)
	}

	inputOptions := InputOptions{}
	err = json.Unmarshal(inputOptionData, &inputOptions)
	if err != nil {
		log.Fatalf("Error unmarshalling mapreduce spec: %v", err)
	}

	mrSpecData := MapReduceSpec{
		UserID:         inputOptions.UserID,
		ContainerName:  inputOptions.ContainerName,
		InputFiles:     []InputFileInfo{},
		NumMapTasks:    inputOptions.NumMapTasks,
		NumReduceTasks: inputOptions.NumReduceTasks,
		MapperFunc:     inputOptions.MapperFunc,
		ReducerFunc:    inputOptions.ReducerFunc,
		JobID:          rand.Int32N(999999),
	}

	// read files information from input directory
	dir, err := os.ReadDir(inputOptions.InputFilesDir)
	if err != nil {
		log.Fatalf("Error reading input directory: %v", err)
	}
	for _, file := range dir {
		if !file.IsDir() {
			fileInfo, err := file.Info()
			if err != nil {
				log.Fatalf("Error reading file info: %v", err)
			}
			mrSpecData.InputFiles = append(mrSpecData.InputFiles, InputFileInfo{
				Name: file.Name(),
				Size: fileInfo.Size(),
			})
		}
	}

	// Encode mrSpecData as JSON
	mrSpecDataJSON, err := json.Marshal(mrSpecData)
	if err != nil {
		log.Fatalf("Error encoding mapreduce spec as JSON: %v", err)
	}

	// retry request
	for i := range 5 {
		log.Printf("MapReduce job specification sent to server, %v", mrSpecData)
		resp, err := http.Post(inputOptions.Url, "application/json", bytes.NewBuffer(mrSpecDataJSON))
		if err != nil {
			log.Printf("Error posting mapreduce spec: %v, retry %d", err, i+1)
			time.Sleep(20 * time.Second)
		} else {
			// Decode the response
			decoder := json.NewDecoder(resp.Body)
			output := outputFiles{}
			err = decoder.Decode(&output)
			if err != nil {
				log.Fatalf("Error decoding response: %v", err)
			}

			log.Printf("Response: %v", output)

			// download output files from azure blob storage
			for _, file := range output.Files {
				err := azBlobHelper.DownloadFile(workingContainer, file, inputOptions.OutputFilesDir)
				if err != nil {
					log.Fatal("Error download output: ", err)
				}
			}
			break
		}
	}

}
