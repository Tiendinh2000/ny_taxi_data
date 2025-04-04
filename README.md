---

docker run -it -e POSTGRES_USER="test" -e POSTGRES_PASSWORD="test" -e POSTGRES_DB=ny_taxi -v D:\download\Data_engineer_zoomcamp\docker_postgresql\ny_taxi_postgres_data:/var/lib/postgresql/data -p 5431:5432 postgres:13

# <!-- cmd: enter pg database with cli -->

pgcli -h localhost -p 5432 -u test -d ny_taxi

# Dataset: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet

# data schema: https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

# run pgadmin4 in container:

docker pull dpage/pgadmin4

docker run -it -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" -e PGADMIN_DEFAULT_PASSWORD="root" -p 8080:80 dpage/pgadmin4

# create network:

docker network create pg_network

docker run -it -e POSTGRES_USER="test" -e POSTGRES_PASSWORD="test" -e POSTGRES_DB=ny_taxi -v D:\download\Data_engineer_zoomcamp\docker_postgresql\ny_taxi_postgres_data:/var/lib/postgresql/data -p 5431:5432 --network=pg_network --name pg-database postgres:13

docker run -it -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" -e PGADMIN_DEFAULT_PASSWORD="root" -p 8080:80 --network=pg_network --name pgadmin dpage/pgadmin4

# run ingest-data.py to ingest+convert data -> pg database

python ingest-data.py --user=test --password=test --host=localhost --port=5432 --db=ny_taxi
--table_name=yellow_taxi_trip --url="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"

# -- dockerizing to ingestion:

docker build -t taxi_ingest:v001 .

# run local as a server for data parquet file -

python -m http.server

# <!-- run ingestion with argv and url is local server -->

docker run -it --network=pg_network taxi_ingest:v001 --user=test --password=test --host=pg-database --port=5432 --db=ny_taxi --table_name=yellow_taxi_trip --url="http://172.27.112.1:8000/output.parquet"



# detach a volume for postgre DB
docker run -it -e POSTGRES_USER="test" -e POSTGRES_PASSWORD="test" -e POSTGRES_DB=ny_taxi -v dtc_postgre_volume_local:/var/lib/postgresql/data -p 5431:5432 --network=pg_network --name pg-database postgres:13