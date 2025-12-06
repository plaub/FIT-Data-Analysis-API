# FIT Data Analysis API

A high-performance FastAPI backend designed to serve fitness data from **Google BigQuery**, featuring **Redis caching** and **Rate Limiting** to optimize performance and control Cloud costs.

## 🚀 Features

- **BigQuery Integration**: Directly queries analytical data from Google BigQuery.
- **Smart Caching**: Uses **Redis** to cache expensive queries (`/api/summary` cached for 1h, `/api/sessions` for 5m).
- **Rate Limiting**: Built-in protection against abuse (configurable per minute).
- **Dockerized**: specific `Dockerfile` and `docker-compose` setup for easy deployment.
- **RESTful API**: Auto-generated Swagger/OpenAPI documentation.

## 🛠️ Prerequisites

- **Docker** & **Docker Compose**
- **Google Cloud Service Account Key**: JSON file with BigQuery Job User and Data Viewer permissions.
- **Python 3.9+** (only if running locally without Docker)

## ⚡ Quick Start

### 1. Setup Environment
Use the included setup script to configure your environment variables:

```bash
python setup.py
```
*This will create a `.env` file based on your inputs.*

### 2. Configure GCP Credentials
Ensure your Service Account Key is placed in the `keys/` directory (or the path you specified in setup).
*Default Docker path corresponds to `./keys/service_account_key.json` on host.*

### 3. Run with Docker
Start the application and Redis:

```bash
docker-compose up --build
```

The API will be available at **http://localhost:8000**.

## 📖 API Documentation

Once running, access the interactive API docs:

- **Swagger UI**: [http://localhost:8000/docs](http://localhost:8000/docs)
- **ReDoc**: [http://localhost:8000/redoc](http://localhost:8000/redoc)

### Key Endpoints
- `GET /health`: Health check (no cache, no limit).
- `GET /api/sessions`: List of recent sessions (Cached: 5m).
- `GET /api/summary`: Global statistics (Cached: 1h).

## 🧪 Development & Testing

Install dependencies locally:
```bash
pip install -r requirements.txt
```

Run automated tests (mocks Redis & BigQuery):
```bash
python -m pytest
```

## 🏗️ Project Structure

```
├── src/
│   ├── main.py           # Application entry point
│   ├── config.py         # Configuration loader
│   ├── dependencies.py   # DI (Redis, BigQuery)
│   ├── models.py         # Pydantic models
│   ├── bigquery_client.py# BigQuery interaction logic
│   └── routers/          # API Route modules
├── tests/                # Pytest tests
├── docker-compose.yml    # Container orchestration
├── Dockerfile            # App container definition
├── setup.py              # Environment setup script
└── requirements.txt      # Python dependencies
```
