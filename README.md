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

## RUN with vsCode dev-container (Recomended)

Install the extension:
VSCode Extension → Dev Containers

Open your folder: Press Ctrl + Shift + P, then:
Dev Containers: Open Folder in Container