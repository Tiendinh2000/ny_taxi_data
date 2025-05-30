
FROM python:3.9

RUN pip install pyarrow fastparquet
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app

COPY ingest-data.py ingest-data.py

ENTRYPOINT [ "python", "ingest-data.py"]