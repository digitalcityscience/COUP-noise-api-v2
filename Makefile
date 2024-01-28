
VENV = .venv
PYTHON = $(VENV)/bin/python3
PIP = $(VENV)/bin/pip

create-env:
	python3 -m venv $(VENV)

venv: create-env
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	$(PIP) install -r requirements-dev.txt
	$(VENV)/bin/pre-commit install
	cp .env.example .env

build:
	docker compose build

start: build
	docker compose up

test-it: build 
	docker compose --env-file .env.example run --rm -it  --entrypoint bash noise-api -c "/bin/bash"
	docker compose down -v

test-docker: build
	docker-compose --env-file .env.example run --rm  noise-api sh -c "sleep 5 && pytest $(pytest-args)"
	docker compose down -v

fmt:
	black ./noise_api/ ./tests/
	isort ./noise_api/ ./tests/

lint:
	black --check ./noise_api/ ./tests/ 
	isort --check ./noise_api/ ./tests/
	flake8 ./noise_api/ ./tests/