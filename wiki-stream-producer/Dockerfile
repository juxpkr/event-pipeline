# Use an official lightweight Python image
FROM python:3.10-slim-bookworm

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file first to leverage Docker's layer caching
COPY wiki-stream-producer/requirements.txt .

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application script into the container
COPY wiki-stream-producer/wiki_country_stream_producer.py .

# Command to run the application.
# The "-u" flag ensures that logs are sent straight to stdout without being buffered.
CMD ["python", "-u", "wiki_country_stream_producer.py"]
