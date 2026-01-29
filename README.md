
# Kafka-Snowflake-Crypto-Analytics
This project is a data pipeline that integrates Apache Kafka, Snowflake, and machine learning for cryptocurrency price analytics. It streams cryptocurrency price data, processes it, and stores the results in a Snowflake data warehouse while utilizing a pre-trained machine learning model for predictions and analysis.

## Table of Contents

- [Features](#features)
- [Technology Stack](#technology-stack)
- [Getting Started](#getting-started)
- [Deployment](#deployment)
- [Configuration](#configuration)

## Features

- Stream cryptocurrency price data using Kafka.
- Process incoming data streams with a Kafka consumer.
- Store processed data in Snowflake for analysis.
- Utilize a pre-trained machine learning model for cryptocurrency price predictions.
- Docker containerization for easy deployment and scalability.
- End-to-end data pipeline from ingestion to analysis.

## Technology Stack

- **Programming Language:** Python
- **Message Streaming:** Apache Kafka
- **Data Warehouse:** Snowflake
- **Containerization:** Docker & Docker Compose
- **Machine Learning:** scikit-learn (or similar for model serialization)

## Getting Started

### Prerequisites

- Docker
- Docker Compose
- Python 3.8 or higher (for local development)

### Clone the Repository

```bash
git clone https://github.com/kavinayac/kafka-snowflake-crypto-analytics.git
cd kafka-snowflake-crypto-analytics
```

## Deployment

To deploy the application, follow these steps:

1. **Build and Run Docker Containers:**

   ```bash
   docker-compose up --build
   ```

2. **Scaling the Application:**

   To scale the number of instances of the crypto analytics service, use:

   ```bash
   docker-compose up --scale crypto-analytics=3
   ```

## Configuration

### Environment Variables

Set the following environment variables in your `docker-compose.yml` file:

- **Kafka Configuration:**
  - `KAFKA_ADVERTISED_LISTENERS`
  - `KAFKA_LISTENERS`
  - `KAFKA_ZOOKEEPER`

- **Snowflake Configuration:**
  - `SNOWFLAKE_ACCOUNT`
  - `SNOWFLAKE_USER`
  - `SNOWFLAKE_PASSWORD`
  - `SNOWFLAKE_DATABASE`
  - `SNOWFLAKE_SCHEMA`

### Docker Configuration

Make sure to adjust your `Dockerfile` and `docker-compose.yml` as needed based on your infrastructure and scaling requirements.

## Usage

After deploying the application, you can start streaming cryptocurrency data. The application will consume this data, process it using the specified machine learning model, and store the results in Snowflake for further analysis.

