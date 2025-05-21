# Base image
FROM python:3.10-slim

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow


# 필요한 패키지 설치
RUN apt-get update && apt-get install -y \
    wget curl unzip gnupg2 ca-certificates \
    libx11-xcb1 libnss3 libxcomposite1 libxcursor1 libxdamage1 libxi6 \
    libxtst6 libxrandr2 libasound2 libpangocairo-1.0-0 libatk1.0-0 \
    libcups2 libxss1 fonts-liberation libappindicator3-1 libatk-bridge2.0-0 \
    libgtk-3-0 libgbm1 xdg-utils

# 최신 Chrome 설치
RUN wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | apt-key add - && \
    echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list && \
    apt-get update && \
    apt-get install -y google-chrome-stable && \
    rm -rf /var/lib/apt/lists/*

# ChromeDriver v136 설치
RUN wget https://storage.googleapis.com/chrome-for-testing-public/136.0.7103.94/linux64/chromedriver-linux64.zip && \
    unzip chromedriver-linux64.zip && \
    mv chromedriver-linux64/chromedriver /usr/bin/chromedriver && \
    chmod +x /usr/bin/chromedriver && \
    rm -rf chromedriver-linux64*

# requirements 설치
COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Airflow + postgre 설치 
RUN pip install apache-airflow==2.8.1 \
    "apache-airflow[postgres]==2.8.1" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.10.txt"

# Copy application code
COPY . /app/

# Set working directory
WORKDIR /opt/airflow
