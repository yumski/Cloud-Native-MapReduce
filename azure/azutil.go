package azutil

import (
	"bytes"
	"context"
	"log"
	"net/url"
	"os"
	"path"

	pb "mapreduce/protos"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

const (
	storageURL    = "https://team17.blob.core.windows.net/"
	containerName = "map-reduce-team17"
	accountKey    = "zLgCuDVQesO0NmAhdN1cwL2mM4Jcm1iKFidl/AX3aTr4Ac5mWfJ32CclcNgDVxkiGfIGWBlJTdpP+AStJLJmAA=="
	accountName   = "team17"
)

func HandleError(err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}

type AzureBlobHelper struct {
	Credential   *azblob.SharedKeyCredential
	Ctx          context.Context
	ContainerURL azblob.ContainerURL
}

func (a *AzureBlobHelper) Init() {
	// initialize the azure credentials
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatal(err.Error())
	}
	a.Credential = credential

	// initialize context
	a.Ctx = context.Background()

	// create the pipeline
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	// create the container URL object
	u, _ := url.Parse(storageURL)
	serviceURL := azblob.NewServiceURL(*u, p)
	a.ContainerURL = serviceURL.NewContainerURL(containerName)
}

// create azure container
func (a *AzureBlobHelper) CreateContainer(containerName string) error {
	if containerName == "" {
		containerName = "map-reduce-team17"
	}

	// Create the container if not exists
	_, err := a.ContainerURL.GetProperties(a.Ctx, azblob.LeaseAccessConditions{})
	if err != nil {
		log.Printf("Creating a container named %s\n", containerName)
		_, err = a.ContainerURL.Create(a.Ctx, azblob.Metadata{}, azblob.PublicAccessNone)
		HandleError(err)
	} else {
		log.Printf("Container %s already exists\n", containerName)
	}
	return nil
}

// upload files to azure container
func (a *AzureBlobHelper) UploadFile(filename string) error {

	// get the file name
	blobName := path.Base(filename)

	// Upload to data to blob storage
	log.Printf("Uploading a blob named %s\n", blobName)

	blobURL := a.ContainerURL.NewBlobURL(blobName)

	_, err := blobURL.GetProperties(a.Ctx, azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		log.Printf("Creating a blob named %s\n", blobName)
		file, err := os.Open(filename)
		HandleError(err)
		defer file.Close()

		_, err = azblob.UploadFileToBlockBlob(a.Ctx, file, a.ContainerURL.NewBlockBlobURL(blobName), azblob.UploadToBlockBlobOptions{})
		HandleError(err)

	} else {
		log.Printf("Blob %s already exists\n", blobName)
	}
	return nil
}

// download files from azure container
func (a *AzureBlobHelper) DownloadFile(filename string) error {

	blobURL := a.ContainerURL.NewBlobURL(filename)

	localFileName := filename

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
func (a *AzureBlobHelper) CreateIntermediateFiles(prefix string, shardInfo []*pb.ShardInfo) error {

	blobName := prefix
	appendblobURL := a.ContainerURL.NewBlockBlobURL(blobName)

	var databuffer bytes.Buffer

	for _, shard := range shardInfo {
		shardBlobURL := a.ContainerURL.NewBlobURL(shard.FileName)
		downloadResponse, err := shardBlobURL.Download(a.Ctx, int64(shard.Start), int64(shard.End), azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})
		HandleError(err)

		bodystream := downloadResponse.Body(azblob.RetryReaderOptions{})
		defer bodystream.Close()

		databuffer.ReadFrom(bodystream)
		HandleError(err)

	}

	hello := []byte("Hello")
	databuffer.Write(hello)

	// upload the byte buffer to blob storage
	_, err := azblob.UploadBufferToBlockBlob(a.Ctx, databuffer.Bytes(), appendblobURL, azblob.UploadToBlockBlobOptions{})
	HandleError(err)

	return nil
}
