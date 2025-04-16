FROM python:3.9.8-slim-buster AS base

# Install Java (OpenJDK 11 is available!)
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    git

# Install Python packages
RUN pip --no-cache-dir install pyspark==3.2.0 \
    python-lsp-server[all] \
    ipykernel

WORKDIR /workspace

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"


COPY . .

CMD python main.py
