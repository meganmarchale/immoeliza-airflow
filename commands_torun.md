List of the commands to run:

# 1. Build your scraper image (optional if only using mounted code)
docker build -t scraper:latest .

# 2. Initialize Airflow DB
docker compose run --rm webserver airflow db init


# 3. Start Airflow
docker-compose up -d
