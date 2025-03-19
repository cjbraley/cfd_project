# install dependencies
init:
	python -m venv ./venv
	./venv/bin/pip install -r requirements.txt

# start docker containers
up:
	docker compose up -d

# stop docker containers
down:
	docker compose down

# execute bronze pipeline
bronze:
	docker exec spark-iceberg bash -c "cd /home/iceberg/ && spark-submit jobs/pipeline_bronze.py"     

# execute silver pipeline
silver:
	docker exec spark-iceberg bash -c "cd /home/iceberg/ && spark-submit jobs/pipeline_silver.py"     

# execute gold pipeline
gold:
	docker exec spark-iceberg bash -c "cd /home/iceberg/ && spark-submit jobs/pipeline_gold.py"     

# execute all data pipelines
etl: bronze silver gold

# execute extract pipeline
extract:
	./venv/bin/python jobs/job_extract.py

# run pytest suite
test:
	./venv/bin/pytest

.PHONY: init up down bronze silver gold etl extract test