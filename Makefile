all: install format lint test build deploy

install:
	pip install --upgrade pip && \
	pip install -r requirements.txt

format:
	black *.py src/*.py

lint:
	pylint --disable=R,C *.py src/*.py

test:
	# test

build:
	# build

run:
	# run

deploy:
	# deploy