# Cloud-Native MapReduce FrameWork

## Prerequisities

* [go](https://golang.org/doc/install)
* [Protocol Buffers](https://developers.google.com/protocol-buffers/docs/gotutorial)
* [kubernetes](https://kubernetes.io/docs/tasks/tools/install-kubectl/)
* [Azure Kubernets Service](https://docs.microsoft.com/en-us/azure/aks/kubernetes-walkthrough)
* [azure-cli](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli)

## Project Overview

This project aims to provide a scalable and efficient MapReduce framework designed to run on Azure Kubernetes Service (AKS) leveraging the power of Go programming language, Docker containers, and gRPC for communication between nodes.

## Usage

1. Submit a MapReduce job to the master node's API with input data location, mapper function, reducer function, and output data location.

2. The master node will distribute the input data across worker nodes, assign map tasks, collect intermediate results, and then assign reduce tasks.

3. Worker nodes will execute map and reduce tasks, storing intermediate results in Azure Storage.

4. Once all tasks are completed, the master node will aggregate the results and store them in the specified output location.
