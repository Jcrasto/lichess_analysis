#!/bin/bash

#aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 737934178320.dkr.ecr.us-east-1.amazonaws.com

#docker image build --tag lichess_analysis_python .
#docker tag lichess_analysis_python 737934178320.dkr.ecr.us-east-1.amazonaws.com/lichess_analysis_python
#docker push 737934178320.dkr.ecr.us-east-1.amazonaws.com/lichess_analysis_python

docker run --rm -v "$HOME/.aws":/root/.aws -v /Users/jcrasto/Projects/lichess_analysis:/app lichess_analysis_python python /app/data_downloader.py

#docker run --rm -v "$HOME/.aws":/root/.aws -v /Users/jcrasto/Projects/lichess_analysis:/app 737934178320.dkr.ecr.us-east-1.amazonaws.com/lichess_analysis_python python /app/scripts/data_downloader.py
