# Fairlane

Distributed task queue engine foundation.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) & [Docker Compose](https://docs.docker.com/compose/)
- `make` (optional, for convenience)

## Quick Start

1. **Configure Environment**

   Copy the example environment file:
   ```bash
   cp .env.example .env
   ```

2. **Start Infrastructure (PostgreSQL & Redis)**

   Using Make:
   ```bash
   make up
   ```

   Or using Docker Compose directly:
   ```bash
   docker compose up -d
   ```

3. **Check Logs**

   ```bash
   make logs
   ```
   Or:
   ```bash
   docker compose logs -f
   ```

4. **Stop Services**

   ```bash
   make down
   ```
   Or:
   ```bash
   docker compose down
   ```

## Services Overview

| Service | Image | Internal Port | Host Port | Healthcheck |
| :--- | :--- | :--- | :--- | :--- |
| **PostgreSQL** | `postgres:16-alpine` | `5432` | `5432` | `pg_isready` |
| **Redis** | `redis:7-alpine` | `6379` | `6379` | `redis-cli ping` |
