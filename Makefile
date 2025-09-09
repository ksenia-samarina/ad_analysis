start:
	docker-compose up -d

stop:
	docker-compose down

start-generator:
	python -m generator.generator

start-producer:
	python -m producer.worker_pool

start-flink-job:
	python -m transformer.streaming_job
