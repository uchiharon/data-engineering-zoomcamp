# Solution to assignment
## Question 1
I edited the parameterization script due to two columns naming conversion.
To build the docker image:
docker image build -t emmanuelikpesu/prefect:zoom_green .
To push image to docker hub:
docker image push emmanuelikpesu/prefect:zoom_green
To Build docker blocker:
python make_docker_block.py
To deploy the work flow and container on prefect:
python docker_deploy.py
code: prefect deployment run etl-parent-flow/docker-flow-green -p "months=[1]" -p "year=2020" -p "color=green"

Answer: 447770

## Question 2
Answer: 0 5 1 * *

## Question 3

Initially, I taugh it was my network that was bad for three days but when I observed the files sizes, I discovered they were large. When I check up the FQA, I discovered it was a timeout error and learned how to set it up.

Answer: 14,851,920

## Question 4
I created the github storage bucket using the UI and also repected it with command line

I built and apply my deployment with the below code:
prefect deployment build week_2_workflow_orchestration/assignment/parameterized_flow_green.py:etl_parent_flow --name docker-flow-green-git -sb github/zoom-flow -a

NOTE: Tips the repo is treated like your localhost, with the first directory as home

Answer: 88605