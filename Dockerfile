# Use the official Python image from the Docker Hub
FROM python:3.11-slim

# Set environment variables to prevent Python from writing pyc files and buffering stdout and stderr
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install virtualenv
RUN pip install --upgrade pip && \
    pip install virtualenv

# Create a virtual environment in the /app/venv directory
RUN virtualenv /app/venv

# Activate the virtual environment and install any dependencies specified in requirements.txt
RUN /app/venv/bin/pip install -r requirements.txt

# Specify the entrypoint command to run your script using the virtual environment's Python interpreter
ENTRYPOINT ["venv/bin/python", "pipeline.py"]