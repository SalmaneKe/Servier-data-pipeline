# Justfile

# Import variables from .env file
set dotenv-load := true

# Variables from .env
PROJECT_ID := env("PROJECT_ID")
REGION := env("REGION")
REPO_NAME := env("REPO_NAME")
IMAGE := "sk-image"
ARGO_APP := "DATA_APP"

# Docker image with variables from .env
DOCKER_IMAGE := "{{REGION}}-docker.pkg.dev/{{PROJECT_ID}}/{{REPO_NAME}}/{{IMAGE}}:latest"

# Authenticate with GCP (run this before pushing)
gcp-auth:
    gcloud auth configure-docker {{REGION}}-docker.pkg.dev
    gcloud auth application-default login

# Install dependencies with Poetry
install:
    poetry install

# Install pre-commit hooks
install-hooks:
    poetry run pre-commit install

# Run pre-commit hooks on all files
pre-commit:
    poetry run pre-commit run --all-files

# Run type checking with mypy
typecheck:
    poetry run mypy main.py processing utils config.py

# Run tests with pytest
test:
    poetry run pytest

# Build the Docker image
docker-build:
    docker build . -t {{DOCKER_IMAGE}}

# Push Docker image to Google Artifact Registry
docker-push:
    just gcp-auth
    docker push {{DOCKER_IMAGE}}

# Deploy to ArgoCD (GKE)
deploy:
    argocd app sync {{ARGO_APP}}

# Full pipeline: install, lint, test, build, push, and deploy
pipeline:
    just install
    just install-hooks
    just lint
    just typecheck
    just test
    just docker-build
    just docker-push
    just deploy
