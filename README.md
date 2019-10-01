# Implementing a Queryable User Profile Store using Kafka Streams

This project provides the source code for our blog post on [Medium](https://medium.com/TODO).
It contains an extensible and queryable user profile store that is updated in real time and built with Kafka Streams.

## Getting Started

There are three modules: the core, recommender, and unified REST API. 
The three modules can be packaged executing `mvn package` in their respective directories. Furthermore, each module contains a Dockerfile, which can be used to build a Docker image by running `docker build .`.

You need to set the corresponding Kafka broker and the Schema Registry URL as command line arguments. Other options and more detailed information can be obtained by executing the jar with the `-h` switch.

## Kubernetes
Additionally, we provide a Kubernetes deployment for the project. The `kubernetes` directory contains the script `install_cluster.sh`. It installs a Kafka cluster including Zookeeper and the Schema Registry on the [Google Kubernetes Engine (GKE)](https://cloud.google.com/kubernetes-engine/). Afterwards, you can execute `install_profile_store.sh` to deploy this project.
The REST API is queryable through its respective LoadBalancer IP. Executing `kubectl describe services profile-store-rest` shows the current address.
