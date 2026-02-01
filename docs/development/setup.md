# Development Setup

## Prerequisites

- **Node.js**: v20+ (recommended)
- **Docker**: For running PostgreSQL, Redis, and ML Service locally.
- **Python**: 3.11+ (if working on ML service locally).

## Installation

1. **Clone the repository**:
   ```bash
   git clone <repo-url>
   cd v4.api.park.fan
   ```

2. **Install dependencies**:
   ```bash
   npm install
   ```

3. **Environment Setup**:
   Copy the example environment file:
   ```bash
   cp .env.example .env
   ```
   *Adjust `.env` variables if necessary (usually defaults work with Docker).*

## Running Locally

1. **Start Infrastructure (DB, Redis, ML)**:
   ```bash
   docker-compose up -d postgres redis ml-service
   ```

2. **Run API in Dev Mode**:
   ```bash
   npm run start:dev
   ```
   The API will be available at `http://localhost:3000`.

3. **Run ML Service Locally (Optional)**:
   If you need to debug Python code:
   ```bash
   cd ml-service
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   uvicorn main:app --reload --port 8000
   ```

## Testing

- **Unit Tests**: `npm run test`
- **E2E Tests**: `npm run test:e2e`
- **Linting**: `npm run lint`

## Python Linting

```bash
cd ml-service
ruff check .
ruff format .
```
