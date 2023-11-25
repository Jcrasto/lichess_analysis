#!/bin/bash

aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 737934178320.dkr.ecr.us-east-1.amazonaws.com

#docker image build --tag lichess_analysis_python -f Dockerfile_Lichess_Analysis .
#docker tag lichess_analysis_python 737934178320.dkr.ecr.us-east-1.amazonaws.com/lichess_analysis_python
#docker push 737934178320.dkr.ecr.us-east-1.amazonaws.com/lichess_analysis_python

#docker run -v "$HOME/.aws":/root/.aws lichess_analysis_python
#docker run -v "$HOME/.aws":/root/.aws 737934178320.dkr.ecr.us-east-1.amazonaws.com/lichess_analysis_python


docker image build --tag pgn_parser_python -f Dockerfile_PGN_Parser .
docker tag pgn_parser_python 737934178320.dkr.ecr.us-east-1.amazonaws.com/pgn_parser_python
docker push 737934178320.dkr.ecr.us-east-1.amazonaws.com/pgn_parser_python

#docker run -v "$HOME/.aws":/root/.aws pgn_parser_python
#docker run -v "$HOME/.aws":/root/.aws 737934178320.dkr.ecr.us-east-1.amazonaws.com/pgn_parser_python