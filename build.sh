#!/bin/bash

echo "Building docker image..."
docker build . --tag fc_mean
