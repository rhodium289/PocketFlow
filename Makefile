.PHONY: help docker-build docker-up docker-down docker-test docker-logs docker-clean

help:
	@echo "PocketFlow Redis System - Docker Commands"
	@echo "========================================="
	@echo "make docker-build   - Build Docker images"
	@echo "make docker-up      - Start all services"
	@echo "make docker-down    - Stop all services"
	@echo "make docker-test    - Run test suite"
	@echo "make docker-logs    - View logs (ctrl+c to exit)"
	@echo "make docker-clean   - Clean up everything"
	@echo "make docker-status  - Show service status"
	@echo "make redis-cli      - Connect to Redis CLI"
	@echo "make create-jobs    - Create test jobs"

docker-build:
	docker-compose build

docker-up:
	docker-compose up -d
	@echo "Services started. Use 'make docker-logs' to view logs"

docker-down:
	docker-compose down

docker-test:
	@echo "Running Redis system tests..."
	docker-compose run --rm test-runner

docker-logs:
	docker-compose logs -f

docker-clean:
	docker-compose down -v --rmi all
	docker system prune -f

docker-status:
	docker-compose ps

redis-cli:
	docker-compose exec redis redis-cli

create-jobs:
	docker-compose exec job-creator python cookbook/pocketflow-redis-example/job_creator.py --job-type batch --batch-size 3 --monitor

# Quick start command
start: docker-build docker-up docker-test
	@echo "PocketFlow Redis system is ready!"
	@echo "Use 'make docker-logs' to view logs"
	@echo "Use 'make create-jobs' to create test jobs"