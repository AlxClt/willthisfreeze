FROM python:3.13.9

WORKDIR /app

# Install dependencies first (better caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the project
COPY . .

# Ensure the /app/data directory exists
RUN mkdir -p /app/data

# Default command (can be overridden by Docker Compose or docker run)
ENTRYPOINT ["python", "main_c2cscraper.py"]
