.PHONY: up down logs check test

up:
	docker compose up --build

down:
	docker compose down

logs:
	docker compose logs -f bot

check:
	python -m compileall -q app tests

test:
	python -m unittest discover -s tests -v
