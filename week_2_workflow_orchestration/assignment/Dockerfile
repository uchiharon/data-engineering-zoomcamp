FROM prefecthq/prefect:2.7.7-python3.9

COPY dockerfile-requirement.txt ./

RUN pip install -r dockerfile-requirement.txt --trusted-host pypi.python.org --no-cache-dir


COPY parameterized_flow_green.py /opt/prefect/flows/parameterized_flow_green.py
COPY data /opt/prefect/data