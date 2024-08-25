FROM python:3.10.14-alpine

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./etl_api /src/etl_api

WORKDIR /src/etl_api

CMD ["python3", "-m" , "flask", "run", "--host=0.0.0.0", "--port=5000"]
