start-local:
	docker-compose up -d

stop-local:
	docker-compose down

init-db:
	python scripts/init_db.py

build-images:
	docker build -t yourrepo/producer:latest ./services/producer
	docker build -t yourrepo/consumer:latest ./services/consumer

deploy-k8s:
	kubectl apply -f k8s/
