FROM python:3.10.14-alpine

WORKDIR /src

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./etl_api /src/etl_api

CMD ["python", "/src/etl_api/main.py"]
