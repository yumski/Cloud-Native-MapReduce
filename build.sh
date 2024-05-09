#!/bin/bash
# Script to build your protobuf, go binaries, and docker images here

protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    protos/mapreduce.proto

docker run --privileged --rm tonistiigi/binfmt --install all
docker buildx create --name sdcc --driver docker-container --use
docker buildx build --load --platform linux/amd64 -f ./docker/Dockerfile.master -t master:latest .
docker buildx build --load --platform linux/amd64 -f ./docker/Dockerfile.worker -t worker:latest .

# docker build -f ./docker/Dockerfile.master -t master:latest .
# docker build -f ./docker/Dockerfile.worker -t worker:latest .

docker tag master:latest myresourcegroupteam17.azurecr.io/master
docker push myresourcegroupteam17.azurecr.io/master:latest

docker tag worker:latest myresourcegroupteam17.azurecr.io/worker
docker push myresourcegroupteam17.azurecr.io/worker:latest

# kubectl delete namespace map-reduce
kubectl create namespace map-reduce

helm repo add bitnami https://charts.bitnami.com/bitnami
helm install -n map-reduce etcd bitnami/etcd --set auth.rbac.create=false

kubectl delete -f kubernetes/worker-deployment.yaml --namespace=map-reduce
kubectl delete -f kubernetes/worker-failure-deployment.yaml --namespace=map-reduce
kubectl delete -f kubernetes/master-deployment.yaml --namespace=map-reduce
kubectl delete -f kubernetes/master-failure-deployment.yaml --namespace=map-reduce
# kubectl delete -f kubernetes/mapreduce-configmap.yaml --namespace=map-reduce

kubectl apply -f kubernetes/mapreduce-configmap.yaml --namespace=map-reduce

kubectl apply -f kubernetes/internal-service.yaml --namespace=map-reduce
kubectl apply -f kubernetes/worker-deployment.yaml --namespace=map-reduce
kubectl apply -f kubernetes/worker-failure-deployment.yaml --namespace=map-reduce

kubectl apply -f kubernetes/external-service.yaml --namespace=map-reduce
kubectl apply -f kubernetes/master-failure-deployment.yaml --namespace=map-reduce
kubectl apply -f kubernetes/master-deployment.yaml --namespace=map-reduce

# set default namespace
kubectl config set-context --current --namespace=map-reduce