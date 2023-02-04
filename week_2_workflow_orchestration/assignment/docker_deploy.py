from prefect.deployments import Deployment
from prefect.infrastructure.docker import DockerContainer
from parameterized_flow_green import etl_parent_flow

docker_block = DockerContainer.load("zoom-green")

docker_dep = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name="docker-flow-green",
    infrastructure=docker_block
)

if __name__=='__main__':
    docker_dep.apply()
