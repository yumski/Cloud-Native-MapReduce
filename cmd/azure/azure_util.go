package azure

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/url"
	"os"
	"os/exec"
	"path"
	"strings"
	"sync"

	"github.com/Azure/azure-storage-blob-go/azblob"

	pb "mapreduce/protos"
)

// will update to using secret.yaml and env variables
const (
	StorageURL       = "https://team17.blob.core.windows.net/"
	ContainerName    = "map-reduce-team17"
	AccountKey       = "zLgCuDVQesO0NmAhdN1cwL2mM4Jcm1iKFidl/AX3aTr4Ac5mWfJ32CclcNgDVxkiGfIGWBlJTdpP+AStJLJmAA=="
	AccountName      = "team17"
	workingContainer = "map-reduce-team17"
)

type AzureBlobHelper struct {
	Credential    *azblob.SharedKeyCredential
	Ctx           context.Context
	ContainerName string
	ServiceURL    azblob.ServiceURL
	ContainerURL  azblob.ContainerURL
}

func HandleError(err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}

func (a *AzureBlobHelper) Init() error {
	// initialize the azure credentials
	credential, err := azblob.NewSharedKeyCredential(AccountName, AccountKey)
	if err != nil {
		return err
	}
	a.Credential = credential

	// initialize context
	a.Ctx = context.Background()

	// create the pipeline
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	// create the container URL object
	u, _ := url.Parse(StorageURL)
	a.ServiceURL = azblob.NewServiceURL(*u, p)
	a.ContainerURL = a.ServiceURL.NewContainerURL(ContainerName)
	return nil
}

func (a *AzureBlobHelper) DownloadBlobData(containerName string, blobName string) ([]byte, error) {
	// connect to specified blob
	containerURL := a.ServiceURL.NewContainerURL(containerName)
	blobURL := containerURL.NewBlobURL(blobName)

	// attempt to download the data
	resp, err := blobURL.Download(a.Ctx, 0, 0, azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return []byte{}, err
	}

	// create and read the data into a buffer
	b := bytes.Buffer{}
	reader := resp.Body(azblob.RetryReaderOptions{})
	b.ReadFrom(reader)
	reader.Close() // The client must close the response body when finished with it

	return b.Bytes(), nil
}

func (a *AzureBlobHelper) UploadBlobData(containerName string, blobName string, data string) error {
	containerURL := a.ServiceURL.NewContainerURL(containerName)

	blobURL := containerURL.NewBlockBlobURL(blobName)

	_, err := blobURL.Upload(
		a.Ctx, strings.NewReader(data), azblob.BlobHTTPHeaders{},
		azblob.Metadata{}, azblob.BlobAccessConditions{},
		azblob.DefaultAccessTier, nil, azblob.ClientProvidedKeyOptions{}, azblob.ImmutabilityPolicyOptions{},
	)

	return err
}

// upload files to azure container
func (a *AzureBlobHelper) UploadFile(containerName string, filename string) error {

	// get the file name
	blobName := path.Base(filename)

	// get container URL
	containerURL := a.ServiceURL.NewContainerURL(containerName)

	blobURL := containerURL.NewBlobURL(blobName)

	_, err := blobURL.GetProperties(a.Ctx, azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		// log.Printf("Creating a blob named %s\n", blobName)
		file, err := os.Open(filename)
		HandleError(err)
		defer file.Close()

		_, err = azblob.UploadFileToBlockBlob(a.Ctx, file, containerURL.NewBlockBlobURL(blobName), azblob.UploadToBlockBlobOptions{})
		HandleError(err)

	} else {
		log.Printf("Blob %s already exists\n", blobName)
	}
	return nil
}

// download files from azure container
func (a *AzureBlobHelper) DownloadFile(containerName string, filename string, localDir string) error {

	containerURL := a.ServiceURL.NewContainerURL(containerName)
	blobURL := containerURL.NewBlobURL(filename)

	localFileName := localDir + "/" + filename

	// download the blob to local file
	file, err := os.Create(localFileName)
	HandleError(err)
	defer file.Close()

	err = azblob.DownloadBlobToFile(a.Ctx, blobURL, 0, 0, file, azblob.DownloadFromBlobOptions{})
	HandleError(err)

	log.Printf("Downloaded %s\n", filename)
	return nil
}

// combin files from azure container
func (a *AzureBlobHelper) CreateIntermediateFiles(containerName string, blobName string, R int32, shardInfo []*pb.ShardInfo) ([]string, error) {

	var cachedFileName string
	var cachedFileData []byte
	intermediateFiles := []string{}
	intermFileData := make([]strings.Builder, R)

	// get custom mapper script from azure blob storage

	customMapperScript, err := a.DownloadBlobData(workingContainer, "mapper.py")
	if err != nil {
		log.Printf("Error downloading custom mapper script: %v", err)
		return []string{}, err
	}

	cmd := exec.Command("python3", "-c", string(customMapperScript))
	stdin, _ := cmd.StdinPipe()

	var wg sync.WaitGroup

	for _, shard := range shardInfo {
		log.Printf("Shard: %v", shard)

		fileName := shard.FileName
		start := shard.Start
		end := shard.End
		bytesRead := start

		log.Printf("filename, start, end : %v, %v, %v", fileName, start, end)

		// check if the file is already cached
		if cachedFileName == "" || cachedFileName != fileName {
			data, err := a.DownloadBlobData(containerName, fileName)
			if err != nil {
				log.Fatal(err)
			}
			cachedFileName = fileName
			cachedFileData = data
		}

		// move the reader to the starting location
		byteReader := bytes.NewReader(cachedFileData)
		byteReader.Seek(int64(start), 0)

		// create a read to read from the byte stream
		reader := bufio.NewReader(byteReader)

		if start != 0 {
			// skip the first line
			line, _ := reader.ReadBytes('\n')
			bytesRead += int32(len(line))
		}

		wg.Add(1)
		// read the rest of lines
		go func() {
			defer wg.Done()
			for bytesRead < end {
				line, err := reader.ReadBytes('\n')
				if err == io.EOF {
					break
				}

				// write the line to the stdin of the mapper
				if _, err := stdin.Write(line); err != nil {
					log.Printf("write to stdin failed, %v", err)
				}
				bytesRead += int32(len(line))
			}
		}()
	}

	// wait for all the goroutines
	go func() {
		defer stdin.Close()
		wg.Wait()
		// log.Println("All files in shard are read and mapped.")
	}()

	out, _ := cmd.Output()

	// read the mapper output, hash the word and place in respective output
	byteReader := bytes.NewReader(out)
	reader := bufio.NewReader(byteReader)

	for {
		// read each mapper result
		line, err := reader.ReadBytes('\n')
		if err == io.EOF {
			break
		}
		// get the word of the line
		word := bytes.Split(line, []byte("\t"))[0]
		// hash the word and modulo by R to get the index
		index := hashWord(word) % uint32(R)
		// write the line to the corresponding index
		intermFileData[index].Write(line)
	}

	// upload each mapper index as a file to blob
	for i, data := range intermFileData {
		blobName := fmt.Sprintf("%v_r%d", blobName, i)
		err := a.UploadBlobData(workingContainer, blobName, data.String())
		if err != nil {
			log.Println(err)
			return nil, err
		}
		intermediateFiles = append(intermediateFiles, blobName)
	}
	log.Printf("Responding with: %v", intermediateFiles)

	return intermediateFiles, nil
}

func hashWord(word []byte) uint32 {
	hash := fnv.New32a()
	hash.Write(word)
	return hash.Sum32()
}

// combin files from azure container
func (a *AzureBlobHelper) CreateOutputFile(containerName string, blobName string, intermFiles []string) (string, error) {

	var databuffer bytes.Buffer
	var wg sync.WaitGroup

	customReducerScript, err := a.DownloadBlobData(workingContainer, "reducer.py")
	if err != nil {
		log.Printf("Error downloading custom reducer script: %v", err)
		return "", err
	}
	cmd := exec.Command("python3", "-c", string(customReducerScript))
	stdin, _ := cmd.StdinPipe()

	for _, file := range intermFiles {

		// download the blob
		data, err := a.DownloadBlobData(containerName, file)
		if err != nil {
			log.Printf("Error downloading intermediate file: %v", err)
			return "", err
		}

		byteReader := bytes.NewReader(data)
		reader := bufio.NewReader(byteReader)

		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				line, err := reader.ReadBytes('\n')
				if err == io.EOF {
					break
				}

				if _, err := stdin.Write(line); err != nil {
					log.Printf("write to stdin failed, %v", err)
				}

			}
		}()

		// wait for all the goroutines
		go func() {
			defer stdin.Close()
			wg.Wait()
			log.Println("All files in shard are read and mapped.")
		}()

	}

	out, _ := cmd.Output()
	databuffer.Write(out)

	// upload the data to the blob
	err = a.UploadBlobData(containerName, blobName, databuffer.String())
	if err != nil {
		log.Println(err)
		return "", err
	}

	return blobName, nil
}
