.PHONY: build clean migrate redis-cli run secret shell stop up

help:
	@echo "Welcome to the Telemetry Airflow\n"
	@echo "The list of commands for local development:\n"
	@echo "  build      Builds the docker images for the docker-compose setup"
	@echo "  clean      Stops and removes all docker containers"
	@echo "  migrate    Runs the Django database migrations"
	@echo "  redis-cli  Opens a Redis CLI"
	@echo "  run        Run a airflow command"
	@echo "  secret     Create a secret to be used for a config variable"
	@echo "  shell      Opens a Bash shell"
	@echo "  up         Runs the whole stack, served under http://localhost:8000/\n"
	@echo "  stop       Stops the docker containers"

build:
	docker-compose build

clean: stop
	docker-compose rm -f

migrate:
	docker-compose run web airflow initdb
	docker-compose run web airflow upgradedb

shell:
	docker-compose run web bash

redis-cli:
	docker-compose run redis redis-cli -h redis

run:
	docker-compose run -e AWS_SECRET_ACCESS_KEY -e AWS_ACCESS_KEY_ID web airflow $(COMMAND)

secret:
	@docker-compose run web python -c \
	"from cryptography.fernet import Fernet; print Fernet.generate_key().decode()"

stop:
	docker-compose down
	docker-compose stop

up:
	docker-compose up
