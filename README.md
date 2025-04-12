# PySpark Docker Project

## Requirements
- Docker (Windows/Mac/Linux)

## Setup

```bash
docker build -t my-spark .
```

## RUN 

```bash
docker build -t my-spark .
```

## RUN with local project mounted (Windows)

```bash
docker run -it --rm -v ${PWD}:/app -w /app my-spark python main.py
```