#!/bin/bash

# Check for correct number of arguments
if [[ $# -ne 3 || ! $1 =~ ^[0-9]+$ ]]; then
  echo "Usage: $0 <number_of_iterations> <topic_name> <broker_address>"
  exit 1
fi

# Get arguments
num_iterations=$1
topic_name=$2
broker_address=$3

# Loop for the specified number of iterations
for (( i=1; i<=$num_iterations; i++ )); do
  # Generate dynamic client ID 
  client_id="Kafka-P$((100 + i))"

  # Updated docker run command
  sudo docker run --rm -d demo-app  \
      --topic-name "$topic_name" \
      --cycle 1000 \
      -c 100000 \
      --client-id "$client_id" \
      --broker "$broker_address"  # New argument

  echo "Iteration: $i with client ID: $client_id" 
done


