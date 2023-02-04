from prefect.infrastructure.docker import DockerContainer

docker_block = DockerContainer(
    image="emmanuelikpesu/prefect:zoom_green",
    image_pull_policy="ALWAYS",
    auto_remove=True,
    
)

docker_block.save('zoom-green', overwrite=True)