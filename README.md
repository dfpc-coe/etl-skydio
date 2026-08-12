<h1 align='center'>ETL-Skydio</h1>

<p align='center'>Skydio Cloud drone telemetry tracking for CloudTAK</p>

Streams live drone positions from [Skydio Cloud](https://cloud.skydio.com) into TAK as Cursor-on-Target
and receives Skydio Webhook events for flight state, device alerts, and live stream status.

## Architecture

Skydio Cloud exposes two complementary realtime interfaces and this ETL uses both:

- **Webhooks** ([docs](https://apidocs.skydio.com/reference/webhook_request_format)) deliver *events* -
  flight start/end, human detected, dock errors, online status, and live stream start/stop. These events
  do **not** contain vehicle positions.
- **Live Telemetry** ([docs](https://apidocs.skydio.com/reference/live-telemetry)) is a websocket that
  publishes vehicle position/attitude/battery at ~5Hz while a vehicle is in flight.

The ETL therefore runs with both invocation types enabled:

- **Schedule** (default `rate(1 minute)`): registers the Webhook with Skydio Cloud (idempotent, matched
  by URL), polls `/v0/vehicles`, and for any vehicle that is `FLYING`/`PREP`/live-streaming connects to
  the Live Telemetry websocket and submits position CoTs every `SUBMIT_INTERVAL` seconds for up to
  `STREAM_DURATION` seconds. With a 1 minute schedule and the default 55s stream window this yields
  continuous tracking.
- **Webhook**: Skydio Cloud POSTs events to the Layer webhook URL:
  - `flight_state` => Layer Alert (Flight Started / Flight Ended)
  - `device_alert` (`HUMAN_DETECTED`, `DOCK_ERROR`, `MISSION_INCOMPLETE`, ...) => Layer Alert
  - `live_stream_status_changed` => RTSP URL stored and attached to the vehicle CoT as a `video` detail
  - `telemetry_available` => optionally submits the full flight track as a LineString (`IMPORT_FLIGHT_TRACK`)
  - `online_status` => logged

Incoming webhook requests are authenticated by validating the `Skydio-Verification` JWT
(HS256, key fetched from `/webhook_validation` and cached) unless `WEBHOOK_VERIFY` is disabled.

## Configuration

| Field | Description |
| ----- | ----------- |
| `SKYDIO_API_TOKEN` | API Token from Skydio Cloud: Settings > API Tokens. Needs read access to Vehicles, Flights, Flight Telemetry, Live Telemetry & read/write access to Webhooks. Live Telemetry is a scope of its own - a token with Flight Telemetry can list vehicles and pull completed flight tracks but will still be rejected by the Live Telemetry websocket. Scopes are fixed when the token is generated, so widening them means issuing a new token |
| `SKYDIO_API_URL` | Skydio Cloud API base - default `https://api.skydio.com/api` |
| `SKYDIO_STREAM_URL` | Live Telemetry websocket base - region specific, default `wss://stream.skydio.com` |
| `WEBHOOK_URL` | Public URL of this Layer's webhook endpoint (webhooks API Gateway base + `/<layer UUID>`). If set, the scheduled run registers it with Skydio Cloud automatically |
| `WEBHOOK_VERIFY` | Validate the `Skydio-Verification` JWT on incoming requests (default `true`) |
| `STREAM_DURATION` | Max seconds of Live Telemetry streaming per scheduled run (default `55`) |
| `SUBMIT_INTERVAL` | Seconds between CoT submissions while streaming (default `2`) |
| `IMPORT_FLIGHT_TRACK` | Submit completed flight tracks as LineStrings (default `false`) |

In Skydio Cloud, Live Telemetry must be enabled: Settings > Live Streaming > Live APIs > Live Telemetry.

## Development

DFPC provided Lambda ETLs are currently all written in [NodeJS](https://nodejs.org/en) through the use of a AWS Lambda optimized
Docker container. Documentation for the Dockerfile can be found in the [AWS Help Center](https://docs.aws.amazon.com/lambda/latest/dg/images-create.html)

```sh
npm install
```

Add a .env file in the root directory that gives the ETL script the necessary variables to communicate with a local ETL server.
When the ETL is deployed the `ETL_API` and `ETL_LAYER` variables will be provided by the Lambda Environment

```json
{
    "ETL_API": "http://localhost:5001",
    "ETL_LAYER": "19"
}
```

To run the task, ensure the local [CloudTAK](https://github.com/dfpc-coe/CloudTAK/) server is running and then run with typescript runtime
or build to JS and run natively with node

```
ts-node task.ts
```

```
npm run build
cp .env dist/
node dist/task.js
```

Run the scheduled flow with `node dist/task.js` or serve the webhook endpoint locally with
`node dist/task.js control:webhooks` (listens on port 5002).

Set `SKYDIO_UNSAFE_URLS=1` to allow `SKYDIO_API_URL` to point at a private/localhost mock during
local development (blocked by SSRF protection otherwise).

A public demo Live Telemetry stream is available for testing: `wss://stream.skydio-dev.com` with any
serial, e.g. `wscat -c wss://stream.skydio-dev.com/data/demo`. It needs no token, so it is a useful
control for isolating a stream failure to credentials rather than to networking or the client.

### Debugging Live Telemetry

Node's `WebSocket` discards the handshake response on any non-101 status, so a rejected upgrade
surfaces only as an empty `ErrorEvent` and a `1006` close. The task therefore replays the failed
handshake over plain HTTPS and logs the status and body Skydio actually returned - the same thing
[the Skydio docs](https://apidocs.skydio.com/reference/live-telemetry) recommend doing by hand with
`curl`. Expect one of:

- `401 API Token was not valid for requested data streams: [data/<serial>]` - the token is missing
  the Live Telemetry scope, Live Telemetry is not enabled for the org, or `SKYDIO_STREAM_URL` points
  at the wrong region. Note this 401 is returned for any serial the token cannot stream, including
  serials that do not exist, so it does not by itself confirm the serial is correct.
- `Provided IDs could not be resolved: [data/<serial>] ...` - the token is fine and the vehicle is
  offline or not streaming.

### Deployment

Deployment into the CloudTAK environment for configuration is done via automatic releases to the DFPC AWS environment.

Github actions will build and push docker releases on every version tag which can then be automatically configured via the 
CloudTAK API.

Non-DFPC users will need to setup their own docker => ECS build system via something like Github Actions or AWS Codebuild.
