set -e

TAXI_TYPE=$1 
YEAR=$2 
#https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet
#https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-01.parquet

URL_PREFIX="https://d37ci6vzurychx.cloudfront.net/trip-data/"

for MONTH in {1..12}; do
    FMONTH=$(printf "%02d" $MONTH)
    URL="${URL_PREFIX}${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.parquet"
    
    LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}"
    LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.parquet"
    LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"
    mkdir -p ${LOCAL_PREFIX}

    wget ${URL} -O ${LOCAL_PATH}
done