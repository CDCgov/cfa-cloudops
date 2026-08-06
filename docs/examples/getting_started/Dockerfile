# Use the official Python 3.11 image
FROM python:3.14

# Set the working directory inside the container if applicable; setting to root here
WORKDIR /

# Install Python dependencies
RUN pip install --no-cache-dir pandas duckdb

# Define the command to run your application
CMD ["python3", "-c", "print('Hello!')"]
