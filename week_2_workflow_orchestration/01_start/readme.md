# Deployment with prefect
we need to specify the file, the entry point flow and the name of the deployment

prefect deployment build ./paramiterized_flow.py:etl_parent_flow -n "Parameterized ETL"

 prefect deployment apply etl_parent_flow-deployment.yaml

prefect deployment build ./paramiterized_flow.py:etl_parent_flow -n "Parameterized ETL" --cron "0 0 * * *"   #build deployment with schedule

prefect deployment build ./paramiterized_flow.py:etl_parent_flow -n "Parameterized ETL" --cron "0 0 * * *" -a #a is to apply