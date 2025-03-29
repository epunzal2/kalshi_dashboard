FROM python:3.9-slim
WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy tickers file
COPY tickers.txt .

# Copy the source code
COPY src/ src/

# Set environment variables
ENV PYTHONPATH=/app
ENV PORT=8080

# Expose the port the app runs on
EXPOSE 8080

# Run the web server
# Use JSON format for CMD to address the warning
# CMD gunicorn --chdir /app --bind 0.0.0.0:${PORT} --workers 1 --threads 8 --timeout 0 src.data_fetcher:app
CMD gunicorn --chdir /app --bind 0.0.0.0:${PORT} --workers 1 --threads 8 --timeout 0 src.minimal_app:app