# ETL-API ᕕ( ᐛ )ᕗ

This project is an ETL (Extract, Transform, Load) API designed to extract data from various sources, transform it, and load it to a specified destination. The project is structured into separate modules for extraction, transformation, and loading of data.

## Running the Code with Docker Compose

To run the ETL API using Docker Compose, follow these steps:

- Install docker with docker compose ([url](https://docs.docker.com/compose/install/))
- Build and Start the Containers:
```sh
docker-compose up
```

## Example API Call

Here is an example of how to call the ETL API:
```sh
curl -X GET http://localhost:5000/api/yahoofinance\?ticker\=AMZN\&period\=1mo
```
​