# Model deployment

## I followed the class instructures

### login to google cloud on cli
- gcloud auth login

### copy the model from bigquery to google storage
- bq --project_id climate-team-1 extract -m trips_data_all.tip_model gs://dtc_data_lake_climate-team-1/tip_model

### copy the model from google cloud storage to my local storage
- gsutil cp -r gs://dtc_data_lake_climate-team-1/tip_model model

### Make a serving_dir 
- mkdir serving_dir/tip_model/1

### copy all the model data into the directory
- cp -r model/tip_model/* serving_dir/tip_model/1

### pull out tensorflow serving docker image
- docker pull tensorflow/serving

### run the docker image
- docker run -p 8501:8501 --mount type=bind,source=D:/uoc/data-engineering-zoomcamp/week_3_data_warehouse/serving_dir/tip_model,target=/models/tip_model -e MODEL_NAME=tip_model -t tensorflow/serving &
