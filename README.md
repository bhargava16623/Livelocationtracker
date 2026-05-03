# Live Location Tracker

A real-time location sharing application where authenticated users can share their GPS position and see other users moving on an interactive map.

## Project Overview

Users log in via OIDC (OAuth 2.0 Authorization Code flow), grant browser location permission, and their position is broadcast to all connected clients in real-time. The system uses Kafka as a message broker for reliable event processing and cross-service communication.

### Architecture

```
Browser (GPS) → Socket.IO → Express Server (port 4000)
                                    ↓
                             Kafka (port 9092)
                               ↙        ↘
                  Server Consumer     Database Processor
                  (broadcasts to      (persists location
                   all clients)        history)
```

## Tech Stack

| Technology | Purpose |
|-----------|---------|
| Node.js + Express | HTTP server and REST API |
| Socket.IO | Real-time bidirectional communication |
| KafkaJS | Kafka producer/consumer in Node.js |
| Apache Kafka | Message broker for location events |
| Leaflet.js | Interactive map rendering |
| OIDC (Custom Auth Server) | Authentication via OAuth 2.0 Authorization Code flow |
| Docker/Podman | Containerized Kafka + Zookeeper |

## Setup Steps

### Prerequisites

- Node.js v18+
- Docker or Podman Desktop
- OIDC Auth Server running (see `oidc-auth-main/` folder)

### 1. Start the OIDC Auth Server

```bash
cd oidc-auth-main
pnpm install
# Start PostgreSQL
podman compose up -d
# Run migrations
npx drizzle-kit push
# Start auth server
pnpm run dev
```

Auth server runs on `http://localhost:8000`.

### 2. Register this app with the Auth Server

```bash
curl -X POST http://localhost:8000/admin/register \
  -H "Content-Type: application/json" \
  -d '{
    "displayName": "Live Location Tracker",
    "applicationUrl": "http://localhost:4000",
    "redirectUri": "http://localhost:4000/auth/callback"
  }'
```

Save the returned `clientId` and `clientSecret`.

### 3. Start Kafka

```bash
cd live-location-tracker
podman compose up -d
```

This starts Zookeeper (port 2181) and Kafka (port 9092).

### 4. Create Kafka Topic

```bash
node kafka-admin.js
```

Creates the `location-updates` topic with 3 partitions.

### 5. Configure Environment

```bash
cp .env.example .env
```

Fill in `CLIENT_ID` and `CLIENT_SECRET` from step 2.

### 6. Install Dependencies & Start

```bash
npm install
npm run dev
```

### 7. Start Database Processor (separate terminal)

```bash
npm run db-processor
```

App runs on `http://localhost:4000`.

## Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `PORT` | Server port | `4000` |
| `APP_URL` | Public URL of this app | `http://localhost:4000` |
| `AUTH_SERVER` | OIDC auth server URL | `http://localhost:8000` |
| `CLIENT_ID` | OAuth client ID (from registration) | `73ef9b8a...` |
| `CLIENT_SECRET` | OAuth client secret (from registration) | `33db0513...` |
| `KAFKA_BROKERS` | Comma-separated Kafka broker addresses | `localhost:9092` |

## OIDC Auth Setup

This app uses a custom OIDC provider (in the `oidc-auth-main/` folder) implementing the **OAuth 2.0 Authorization Code** flow:

1. User clicks "Sign in with OIDC"
2. App redirects to `AUTH_SERVER/o/authorize?client_id=...&redirect_uri=...&response_type=code&state=...`
3. User logs in / signs up on the auth server
4. Auth server redirects back to `/auth/callback?code=...&state=...`
5. App exchanges code for tokens via `POST /o/token` (server-to-server)
6. `access_token` and `id_token` stored as httpOnly cookies
7. Socket.IO reads cookies during WebSocket handshake for authentication

## Socket Event Flow

### Client → Server

| Event | Payload | Description |
|-------|---------|-------------|
| `client:location-update` | `{ lat: number, lng: number }` | User's GPS position |

### Server → Client

| Event | Payload | Description |
|-------|---------|-------------|
| `server:initial-locations` | `[{ userId, lat, lng, name, timestamp }]` | All active users on connect |
| `server:location-update` | `{ userId, lat, lng, name, timestamp }` | Live position update |
| `server:user-disconnected` | `{ userId }` | User went offline |
| `server:error` | `{ error: string }` | Validation or rate limit error |

### Connection Flow

```
Client connects → Socket middleware validates access_token cookie
                → Sends server:initial-locations with all active positions
                → Client starts watchPosition() and emits client:location-update
                → Server publishes to Kafka
                → Kafka consumer broadcasts server:location-update to all
```

## Kafka Event Flow

### Topic: `location-updates` (3 partitions)

**Message Format:**
```json
{
  "userId": "uuid",
  "name": "John Doe",
  "lat": 37.7749,
  "lng": -122.4194,
  "timestamp": 1714737600000
}
```

### Consumer Groups

| Group ID | Process | Purpose |
|----------|---------|---------|
| `location-broadcast` | Main server (`index.js`) | Broadcasts to connected WebSocket clients |
| `location-db-processor` | `database-processor.js` | Persists location history for logging |

### Deduplication

Events are deduplicated by `userId + timestamp` — if the same timestamp is seen again for a user, it's skipped.

### Key-based Partitioning

Messages are keyed by `userId`, ensuring all events from the same user go to the same partition (maintains ordering per user).

## Demo Video Link

> https://youtu.be/QDaIvLdnxUw?si=IDibnLFpaN2Lp75J

## Assumptions and Limitations

- **Single Kafka broker**: Uses one broker for local development; production would use a cluster
- **In-memory state**: Active user positions are stored in a `Map`; a server restart loses current positions (Kafka retains events for replay)
- **Database processor simulates persistence**: Stores in an array; replace with PostgreSQL/MongoDB for production
- **Browser geolocation required**: Users must grant location permission; accuracy depends on device GPS
- **No rate limiting on location updates**: In production, add throttling (e.g., max 1 update per second)
- **Single server instance**: Kafka Pub/Sub allows horizontal scaling, but current setup runs one instance
- **HTTPS required in production**: Geolocation API requires secure context in modern browsers
- **Token expiry**: When JWT expires, user must re-login (no refresh token implemented)
