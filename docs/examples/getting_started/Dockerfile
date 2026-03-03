# Use the official Python 3.11 image
FROM python:3.11

# Set the working directory inside the container
WORKDIR /app

# Install Python dependencies
RUN pip install --no-cache-dir pandas duckdb

# Define the command to run your application
CMD ["python3", "-c", "print('Hello!')"]
