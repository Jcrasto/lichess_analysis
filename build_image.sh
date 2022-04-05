#!/bin/bash

aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 737934178320.dkr.ecr.us-east-1.amazonaws.com

docker image build --tag lichess_analysis_python ./image
docker tag lichess_analysis_python 737934178320.dkr.ecr.us-east-1.amazonaws.com/lichess_analysis_python
docker push 737934178320.dkr.ecr.us-east-1.amazonaws.com/lichess_analysis_python