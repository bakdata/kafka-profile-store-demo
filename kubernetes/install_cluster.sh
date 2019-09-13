#!/usr/bin/env bash

export NAME=profilestore-gke
export ZONE=us-central1-a
export REGION=$(echo ${ZONE} | cut -d'-' -f-2)

gcloud container clusters create $NAME --zone=$ZONE --num-nodes=3 --machine-type=n1-standard-2
gcloud config set container/cluster ${NAME}

echo "Create kubernetes dashboard"
kubectl create clusterrolebinding cluster-admin-binding --clusterrole cluster-admin --user "$(gcloud config get-value account)"
kubectl create -f https://raw.githubusercontent.com/kubernetes/kops/master/addons/kubernetes-dashboard/v1.8.3.yaml
kubectl apply -f dashboard_access.yaml

echo -e "Setup Helm"
kubectl create -f helm_rbac_config.yaml
helm init --upgrade --service-account tiller

echo "Allocate load balancer ips"
gcloud compute addresses create ${KAFKA_IP_NAME_0} --region ${REGION}
gcloud compute addresses create ${KAFKA_IP_NAME_1} --region ${REGION}
gcloud compute addresses create ${KAFKA_IP_NAME_2} --region ${REGION}
gcloud compute addresses create ${SR_IP_NAME} --region ${REGION}

KAFKA_IP_0="$(gcloud compute addresses describe ${KAFKA_IP_NAME_0} --region ${REGION} --format='value(address)')"
KAFKA_IP_1="$(gcloud compute addresses describe ${KAFKA_IP_NAME_1} --region ${REGION} --format='value(address)')"
KAFKA_IP_2="$(gcloud compute addresses describe ${KAFKA_IP_NAME_2} --region ${REGION} --format='value(address)')"
SR_IP="$(gcloud compute addresses describe ${SR_IP_NAME} --region ${REGION} --format='value(address)')"

echo "Setup Kafka"
cat kafka.yaml | sed 's/\${KAFKA_IP_0}'"/$KAFKA_IP_0/g" |sed 's/\${KAFKA_IP_1}'"/$KAFKA_IP_1/g" |sed 's/\${KAFKA_IP_2}'"/$KAFKA_IP_2/g" |helm install --name kafka incubator/kafka -f -

echo "Setup Schema Registry"
cat schema_registry.yaml | sed 's/\${SR_IP}'"/$SR_IP/g" | helm install --name ps incubator/schema-registry -f