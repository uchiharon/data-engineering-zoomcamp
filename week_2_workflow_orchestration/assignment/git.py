from prefect.filesystems import GitHub

github_block = GitHub.load("zoom-flow")
github_block.get_directory("week_2_workflow_orchestration/assignment")
github_block.save("dev")
