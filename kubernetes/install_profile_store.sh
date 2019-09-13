#!/usr/bin/env bash

kubectl create -f profile_store_config.yaml
kubectl create -f profile_store.yaml
kubectl create -f recommender.yaml
kubectl create -f rest.yaml
