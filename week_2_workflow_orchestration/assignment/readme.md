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
0 5 1 * *

## Question 3
