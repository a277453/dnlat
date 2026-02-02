# -------------------------------
# Base image
# -------------------------------
FROM python:3.12-slim

# -------------------------------
# System dependencies
# -------------------------------
RUN apt-get update && apt-get install -y \
    gcc \
    libxml2 \
    libxslt1-dev \
    && rm -rf /var/lib/apt/lists/*


# Install Ollama CLI
RUN curl -fsSL https://ollama.com/install.sh | bash

# -------------------------------
# Environment settings
# -------------------------------
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# -------------------------------
# Working directory
# -------------------------------
WORKDIR /app

# -------------------------------
# Install Python dependencies
# -------------------------------
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# -------------------------------
# Copy application code
# -------------------------------
COPY . .

# -------------------------------
# Create runtime directories
# -------------------------------
RUN mkdir -p /app/tmp /app/logs

# -------------------------------
# Expose FastAPI port
# -------------------------------
EXPOSE 8000
EXPOSE 8501
EXPOSE 11434
# -------------------------------
# Start FastAPI
# -------------------------------
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
