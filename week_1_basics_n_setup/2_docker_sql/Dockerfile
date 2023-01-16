FROM python:3.9

RUN pip install pandas \
    pip install openpyxl

WORKDIR /app
COPY pipeline.py customer-list.xlsx ./


ENTRYPOINT [ "python", "pipeline.py" ]