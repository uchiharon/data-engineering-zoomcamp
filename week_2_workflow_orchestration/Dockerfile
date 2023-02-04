FROM prefecthq/prefect:2.7.7-python3.9

COPY 03_deployment/dockerfile-requirement.txt ./

RUN pip install -r dockerfile-requirement.txt --trusted-host pypi.python.org --no-cache-dir


COPY 01_start/paramiterized_flow.py /opt/prefect/flows/paramiterized_flow.py
COPY 01_start/data /opt/prefect/data