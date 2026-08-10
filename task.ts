import crypto from 'node:crypto';
import type { Static, TSchema } from '@sinclair/typebox';
import { Type } from '@sinclair/typebox';
import type { Event } from '@tak-ps/etl';
import type Schema from '@openaddresses/batch-schema';
import ETL, { SchemaType, handler as internal, local, fetch, Feature, DataFlowType, InvocationType } from '@tak-ps/etl';

/**
 * The Input Schema contains the environment object that will be requested via the CloudTAK UI
 * It should be a valid TypeBox object - https://github.com/sinclairzx81/typebox
 */
const InputSchema = Type.Object({
    'SKYDIO_API_TOKEN': Type.String({
        description: 'Skydio Cloud API Token - Generated in Skydio Cloud: Settings > API Tokens'
    }),
    'SKYDIO_API_URL': Type.String({
        default: 'https://api.skydio.com/api',
        description: 'Skydio Cloud API Base URL'
    }),
    'SKYDIO_STREAM_URL': Type.String({
        default: 'wss://stream.skydio.com',
        description: 'Skydio Live Telemetry Websocket Base URL - Region specific, shown in Skydio Cloud: Settings > Devices > Vehicles > Connectivity'
    }),
    'WEBHOOK_URL': Type.Optional(Type.String({
        description: 'Public HTTPS URL of this Layer\'s Webhook endpoint. If set, the scheduled run will automatically register it with Skydio Cloud'
    })),
    'WEBHOOK_VERIFY': Type.Boolean({
        default: true,
        description: 'Verify the Skydio-Verification JWT on incoming webhook requests'
    }),
    'STREAM_DURATION': Type.Integer({
        default: 55,
        description: 'Maximum number of seconds to stream Live Telemetry per scheduled invocation - should be less than both the Layer Timeout and the schedule interval'
    }),
    'SUBMIT_INTERVAL': Type.Integer({
        default: 2,
        description: 'Number of seconds between CoT submissions while streaming Live Telemetry'
    }),
    'IMPORT_FLIGHT_TRACK': Type.Boolean({
        default: false,
        description: 'Submit the full flight track as a Line once telemetry for a completed flight has been uploaded to Skydio Cloud'
    }),
    'DEBUG': Type.Boolean({
        default: false,
        description: 'Print results in logs'
    })
});

/**
 * The Output Schema contains the known properties that will be returned on the
 * GeoJSON Feature in the .properties.metdata object
 */
const OutputSchema = Type.Object({
    serial: Type.String({ description: 'Skydio Vehicle Serial' }),
    nickname: Type.Optional(Type.String({ description: 'Vehicle display name assigned in Skydio Cloud' })),
    vehicle_class: Type.Optional(Type.String({ description: 'Vehicle model - ie Skydio X10' })),
    pilot: Type.Optional(Type.String({ description: 'Current pilot email' })),
    battery: Type.Optional(Type.Number({ description: 'Battery percentage (0-100)' })),
    rssi: Type.Optional(Type.Number({ description: 'Received signal strength indicator' })),
    alt_msl: Type.Optional(Type.Number({ description: 'GPS altitude in meters above mean sea level' })),
    alt_above_launch: Type.Optional(Type.Number({ description: 'Altitude above the launch point in meters' })),
    gps_satellites_used: Type.Optional(Type.Number({ description: 'Number of satellites used for GPS' })),
    mission_name: Type.Optional(Type.String({ description: 'Name of the running mission' })),
    mission_state: Type.Optional(Type.String({ description: 'State of the running mission' }))
});

const EphemeralSchema = Type.Object({
    webhook_id: Type.Optional(Type.String()),
    webhook_url: Type.Optional(Type.String()),
    streams: Type.Optional(Type.Record(Type.String(), Type.Object({
        rtsp_url: Type.String(),
        stream_type: Type.String()
    })))
});

const SkydioWebhook = Type.Object({
    id: Type.String(),
    name: Type.String(),
    url: Type.String()
});

const SkydioVehicle = Type.Object({
    vehicle_serial: Type.String(),
    vehicle_class: Type.Optional(Type.String()),
    name: Type.Optional(Type.String()),
    flight_status: Type.Optional(Type.String()),
    is_online: Type.Optional(Type.Boolean()),
    is_online_via_mobile: Type.Optional(Type.Boolean()),
    is_live_streaming: Type.Optional(Type.Boolean()),
    battery_status: Type.Optional(Type.Object({
        charging: Type.Optional(Type.Boolean()),
        percentage: Type.Optional(Type.Number())
    }, { additionalProperties: true }))
}, { additionalProperties: true });

/**
 * Message published at ~5Hz by the Skydio Live Telemetry Websocket
 * https://apidocs.skydio.com/reference/live-telemetry
 */
const LiveStatus = Type.Object({
    type: Type.Optional(Type.String()),
    tm: Type.Optional(Type.Number()),
    serial: Type.String(),
    rssi: Type.Optional(Type.Number()),
    battery: Type.Optional(Type.Number()),
    pilot: Type.Optional(Type.String()),
    nickname: Type.Optional(Type.String()),
    speed: Type.Optional(Type.Number()),
    alt_above_launch: Type.Optional(Type.Number()),
    alt_msl: Type.Optional(Type.Number()),
    lat: Type.Number(),
    lon: Type.Number(),
    gps_satellites_used: Type.Optional(Type.Number()),
    roll: Type.Optional(Type.Number()),
    pitch: Type.Optional(Type.Number()),
    yaw: Type.Optional(Type.Number()),
    gimbal_roll: Type.Optional(Type.Number()),
    gimbal_pitch: Type.Optional(Type.Number()),
    gimbal_yaw: Type.Optional(Type.Number()),
    mission: Type.Optional(Type.Object({
        state: Type.Optional(Type.String()),
        name: Type.Optional(Type.String()),
        waypoint_index: Type.Optional(Type.Number()),
        total_waypoints: Type.Optional(Type.Number())
    }, { additionalProperties: true }))
}, { additionalProperties: true });

/**
 * Webhook Request Envelope
 * https://apidocs.skydio.com/reference/webhook_request_format
 */
const WebhookEvent = Type.Object({
    id: Type.Optional(Type.String()),
    event_type: Type.String(),
    event_time: Type.Optional(Type.String()),
    data: Type.Object({
        resource: Type.Record(Type.String(), Type.Unknown())
    })
}, { additionalProperties: true });

const FlightStateResource = Type.Object({
    type: Type.String(),
    time: Type.Optional(Type.String()),
    flight_id: Type.Optional(Type.String()),
    vehicle_serial: Type.String()
}, { additionalProperties: true });

const DeviceAlertResource = Type.Object({
    alert_type: Type.String(),
    alert_time: Type.Optional(Type.String()),
    device: Type.Optional(Type.Object({
        type: Type.Optional(Type.String()),
        id: Type.Optional(Type.String())
    }, { additionalProperties: true })),
    mission_result: Type.Optional(Type.String()),
    dock_error_type: Type.Optional(Type.String())
}, { additionalProperties: true });

const OnlineStatusResource = Type.Object({
    time: Type.Optional(Type.String()),
    device: Type.Optional(Type.Object({
        type: Type.Optional(Type.String()),
        id: Type.Optional(Type.String())
    }, { additionalProperties: true })),
    is_online: Type.Boolean()
}, { additionalProperties: true });

const LiveStreamResource = Type.Object({
    live_stream_status: Type.String(),
    rtsp_url: Type.Optional(Type.String()),
    stream_type: Type.Optional(Type.String()),
    vehicle_serial: Type.String()
}, { additionalProperties: true });

const TelemetryAvailableResource = Type.Object({
    file_uuid: Type.Optional(Type.String()),
    flight_id: Type.String(),
    vehicle_serial: Type.Optional(Type.String())
}, { additionalProperties: true });

/**
 * Convert an East-North-Up yaw (radians, 0 = East, counter-clockwise positive)
 * to a compass heading (degrees, 0 = North, clockwise positive)
 */
function enuToCompass(yaw: number): number {
    return ((90 - (yaw * 180 / Math.PI)) % 360 + 360) % 360;
}

function statusToFeature(
    status: Static<typeof LiveStatus>,
    opts: {
        vehicle?: Static<typeof SkydioVehicle>,
        stream?: { rtsp_url: string, stream_type: string }
    } = {}
): Static<typeof Feature.InputFeature> {
    const callsign = status.nickname || opts.vehicle?.name || status.serial;

    const remarks = [`Serial: ${status.serial}`];
    if (opts.vehicle?.vehicle_class) remarks.push(`Model: ${opts.vehicle.vehicle_class}`);
    if (status.pilot) remarks.push(`Pilot: ${status.pilot}`);
    if (status.battery !== undefined) remarks.push(`Battery: ${Math.round(status.battery * 100)}%`);
    if (status.mission && status.mission.name) {
        remarks.push(`Mission: ${status.mission.name}${status.mission.state ? ` (${status.mission.state})` : ''}`);
    }

    const metadata: Static<typeof OutputSchema> = {
        serial: status.serial,
        nickname: status.nickname,
        vehicle_class: opts.vehicle?.vehicle_class,
        pilot: status.pilot,
        battery: status.battery !== undefined ? Math.round(status.battery * 100) : undefined,
        rssi: status.rssi,
        alt_msl: status.alt_msl,
        alt_above_launch: status.alt_above_launch,
        gps_satellites_used: status.gps_satellites_used,
        mission_name: status.mission ? status.mission.name : undefined,
        mission_state: status.mission ? status.mission.state : undefined
    };

    const feat: Static<typeof Feature.InputFeature> = {
        id: `skydio-${status.serial}`,
        type: 'Feature',
        properties: {
            type: 'a-f-A-M-H-Q',
            callsign,
            remarks: remarks.join('\n'),
            metadata
        },
        geometry: {
            type: 'Point',
            coordinates: [status.lon, status.lat, status.alt_msl ?? 0]
        }
    };

    if (status.speed !== undefined) feat.properties.speed = status.speed;
    if (status.yaw !== undefined) feat.properties.course = enuToCompass(status.yaw);

    // Gimbal orientation is published in ENU - project it as a Sensor FoV cone
    // Range/FoV are not published so representative constants are used
    if (status.gimbal_yaw !== undefined) {
        feat.properties.sensor = {
            azimuth: enuToCompass(status.gimbal_yaw),
            elevation: status.gimbal_pitch !== undefined ? status.gimbal_pitch * 180 / Math.PI : 0,
            fov: 45,
            range: 100
        };
    }

    if (opts.stream) {
        feat.properties.video = {
            uid: `skydio-${status.serial}`,
            sensor: `${callsign}-${opts.stream.stream_type}`,
            url: opts.stream.rtsp_url,
            connection: {
                uid: `skydio-${status.serial}`,
                networkTimeout: 12000,
                path: '',
                protocol: 'raw',
                bufferTime: -1,
                address: opts.stream.rtsp_url,
                port: -1,
                roverPort: -1,
                rtspReliable: 0,
                ignoreEmbeddedKLV: false,
                alias: callsign
            }
        };
    }

    return feat;
}

// JWK secrets are cached per key id for the lifetime of the Lambda container
const JWKCache: Map<string, Buffer> = new Map();

export default class Task extends ETL {
    static name = 'etl-skydio'
    static flow = [ DataFlowType.Incoming ];
    static invocation = [ InvocationType.Schedule, InvocationType.Webhook ];
    static invocationDefaults = {
        webhook: { enabled: true },
        schedule: { enabled: true, cron: 'rate(1 minute)' }
    };

    async schema(
        type: SchemaType = SchemaType.Input,
        flow: DataFlowType = DataFlowType.Incoming
    ): Promise<TSchema> {
        if (flow === DataFlowType.Incoming) {
            if (type === SchemaType.Input) {
                return InputSchema;
            } else {
                return OutputSchema;
            }
        } else {
            return Type.Object({});
        }
    }

    /**
     * Perform an authenticated request against the Skydio Cloud API,
     * unwrapping the standard `{ data: ... }` response envelope
     */
    async skydio<T extends TSchema>(
        env: Static<typeof InputSchema>,
        path: string,
        schema: T,
        opts: {
            method?: string;
            body?: object;
        } = {}
    ): Promise<Static<T>> {
        const url = new URL(env.SKYDIO_API_URL.replace(/\/+$/, '') + path);

        const headers: Record<string, string> = {
            'Authorization': env.SKYDIO_API_TOKEN,
            'Accept': 'application/json'
        };

        if (opts.body) headers['Content-Type'] = 'application/json';

        const res = await fetch(url, {
            method: opts.method || 'GET',
            headers,
            body: opts.body ? JSON.stringify(opts.body) : undefined,
            // Local development escape hatch - node-safeurl blocks private hostnames by default
            safeUrlAllow: process.env.SKYDIO_UNSAFE_URLS ? [url.origin] : undefined
        });

        if (!res.ok) {
            throw new Error(`Skydio API ${opts.method || 'GET'} ${path} failed (${res.status}): ${await res.text()}`);
        }

        // All Skydio responses use a standard { data, meta, status_code, ... } envelope
        const envelope: unknown = await res.json();
        if (!envelope || typeof envelope !== 'object' || !('data' in envelope)) {
            throw new Error(`Skydio API ${opts.method || 'GET'} ${path} returned an unexpected response envelope`);
        }

        return this.type(schema, (envelope as { data: unknown }).data);
    }

    /**
     * Register this Layer's Webhook endpoint with Skydio Cloud if it isn't already.
     * Matching is done by URL so the operation is idempotent and self-heals if the
     * webhook is deleted in Skydio Cloud
     */
    async ensureWebhook(env: Static<typeof InputSchema>): Promise<void> {
        if (!env.WEBHOOK_URL) return;

        const eph = await this.ephemeral(EphemeralSchema);

        const list = await this.skydio(env, '/v0/webhooks', Type.Object({
            webhooks: Type.Optional(Type.Array(SkydioWebhook))
        }, { additionalProperties: true }));

        const existing = (list.webhooks || []).find((hook) => hook.url === env.WEBHOOK_URL);

        if (existing) {
            if (eph.webhook_id !== existing.id || eph.webhook_url !== existing.url) {
                await this.setEphemeral({ ...eph, webhook_id: existing.id, webhook_url: existing.url });
            }
        } else {
            const created = await this.skydio(env, '/v0/webhook', Type.Object({
                webhook: SkydioWebhook
            }, { additionalProperties: true }), {
                method: 'POST',
                body: {
                    name: `CloudTAK Layer ${this.etl.layer}`,
                    url: env.WEBHOOK_URL
                }
            });

            console.log(`ok - registered webhook ${created.webhook.id} => ${created.webhook.url}`);
            await this.setEphemeral({ ...eph, webhook_id: created.webhook.id, webhook_url: created.webhook.url });
        }
    }

    async vehicles(env: Static<typeof InputSchema>): Promise<Static<typeof SkydioVehicle>[]> {
        const vehicles: Static<typeof SkydioVehicle>[] = [];

        let page = 1;
        let total_pages: number;
        do {
            const data = await this.skydio(env, `/v0/vehicles?per_page=100&page_number=${page}`, Type.Object({
                pagination: Type.Object({
                    current_page: Type.Integer(),
                    total_pages: Type.Integer(),
                    max_per_page: Type.Integer()
                }),
                vehicles: Type.Optional(Type.Array(SkydioVehicle))
            }, { additionalProperties: true }));

            vehicles.push(...(data.vehicles || []));
            total_pages = data.pagination.total_pages;
            ++page;
        } while (page <= total_pages);

        return vehicles;
    }

    /**
     * Connect to the Skydio Live Telemetry Websocket for the given vehicles and submit
     * position CoTs as they arrive until the stream closes or the duration budget is spent
     *
     * https://apidocs.skydio.com/reference/live-telemetry
     */
    async stream(
        env: Static<typeof InputSchema>,
        vehicles: Static<typeof SkydioVehicle>[],
        duration: number
    ): Promise<void> {
        const byserial = new Map(vehicles.map((vehicle) => [vehicle.vehicle_serial, vehicle]));
        const eph = await this.ephemeral(EphemeralSchema);
        const streams = eph.streams || {};

        const url = new URL('/data', env.SKYDIO_STREAM_URL);
        url.searchParams.set('skydioSerials', Array.from(byserial.keys()).join(','));
        url.searchParams.set('token', env.SKYDIO_API_TOKEN);

        console.log(`ok - streaming live telemetry for ${Array.from(byserial.keys()).join(', ')} for up to ${Math.round(duration / 1000)}s`);

        const latest: Map<string, Static<typeof LiveStatus>> = new Map();
        let dirty = false;
        let submitting = false;

        const flush = async (): Promise<void> => {
            if (!dirty || submitting) return;
            dirty = false;
            submitting = true;

            try {
                const fc: Static<typeof Feature.InputFeatureCollection> = {
                    type: 'FeatureCollection',
                    features: Array.from(latest.values()).map((status) => {
                        return statusToFeature(status, {
                            vehicle: byserial.get(status.serial),
                            stream: streams[status.serial]
                        });
                    })
                };

                if (env.DEBUG) console.log(JSON.stringify(fc));

                await this.submit(fc);
            } catch (err) {
                console.error(err);
            } finally {
                submitting = false;
            }
        };

        await new Promise<void>((resolve) => {
            const ws = new WebSocket(url);

            let interval: ReturnType<typeof setInterval> | undefined = undefined;
            let deadline: ReturnType<typeof setTimeout> | undefined = undefined;
            let done = false;

            const finish = (): void => {
                if (done) return;
                done = true;

                if (interval !== undefined) clearInterval(interval);
                if (deadline !== undefined) clearTimeout(deadline);

                try {
                    ws.close();
                } catch (err) {
                    console.error(err);
                }

                resolve();
            };

            deadline = setTimeout(finish, duration);
            interval = setInterval(flush, env.SUBMIT_INTERVAL * 1000);

            ws.addEventListener('open', () => {
                console.log('ok - live telemetry websocket connected');
            });

            ws.addEventListener('message', (msg) => {
                if (typeof msg.data !== 'string') return;

                try {
                    const status = this.type(LiveStatus, JSON.parse(msg.data));
                    if (status.type && status.type !== 'status') return;

                    latest.set(status.serial, status);
                    dirty = true;
                } catch (err) {
                    if (env.DEBUG) console.error(err);
                }
            });

            ws.addEventListener('error', (err) => {
                console.error('not ok - live telemetry websocket error', err);
                finish();
            });

            ws.addEventListener('close', (ev) => {
                console.log(`ok - live telemetry websocket closed: ${ev.code} ${ev.reason}`);
                finish();
            });
        });

        await flush();
    }

    /**
     * Fetch the uploaded telemetry log for a completed flight and submit it as a LineString track
     */
    async importFlightTrack(env: Static<typeof InputSchema>, flight_id: string, vehicle_serial?: string): Promise<void> {
        const data = await this.skydio(env, `/v1/flight/${encodeURIComponent(flight_id)}/telemetry`, Type.Object({
            flight_telemetry: Type.Object({
                aligned_telemetry: Type.Array(Type.Object({
                    timestamp: Type.String(),
                    gps_latitude: Type.Optional(Type.Number()),
                    gps_longitude: Type.Optional(Type.Number()),
                    gps_altitude: Type.Optional(Type.Number())
                }, { additionalProperties: true }))
            }, { additionalProperties: true })
        }, { additionalProperties: true }));

        const positions = data.flight_telemetry.aligned_telemetry
            .filter((entry) => entry.gps_latitude !== undefined && entry.gps_longitude !== undefined);

        if (positions.length < 2) {
            console.log(`ok - flight ${flight_id} has insufficient GPS telemetry for a track`);
            return;
        }

        // Limit tracks to ~1000 vertices to keep the CoT a reasonable size
        const step = Math.max(1, Math.ceil(positions.length / 1000));
        const coordinates = positions
            .filter((_, idx) => idx % step === 0 || idx === positions.length - 1)
            .map((entry) => [entry.gps_longitude!, entry.gps_latitude!, entry.gps_altitude ?? 0]);

        const fc: Static<typeof Feature.InputFeatureCollection> = {
            type: 'FeatureCollection',
            features: [{
                id: `skydio-${flight_id}-track`,
                type: 'Feature',
                properties: {
                    type: 'u-d-f',
                    callsign: `${vehicle_serial || 'Skydio'} Flight Track`,
                    remarks: `Flight: ${flight_id}\nVehicle: ${vehicle_serial || 'Unknown'}\nStart: ${positions[0].timestamp}\nEnd: ${positions[positions.length - 1].timestamp}`,
                    stroke: '#00E0E0',
                    'stroke-width': 3,
                    'stroke-opacity': 1
                },
                geometry: {
                    type: 'LineString',
                    coordinates
                }
            }]
        };

        if (env.DEBUG) console.log(JSON.stringify(fc));

        await this.submit(fc);
    }

    /**
     * Validate the Skydio-Verification JWT on an incoming webhook request
     * https://apidocs.skydio.com/reference/webhook-validation
     */
    async verify(env: Static<typeof InputSchema>, token: unknown, body: unknown): Promise<void> {
        if (!token || typeof token !== 'string') throw new Error('Missing Skydio-Verification header');

        const parts = token.split('.');
        if (parts.length !== 3) throw new Error('Malformed Skydio-Verification JWT');

        const header: unknown = JSON.parse(Buffer.from(parts[0], 'base64url').toString());
        const { alg, kid } = this.type(Type.Object({
            alg: Type.String(),
            kid: Type.String()
        }, { additionalProperties: true }), header);

        if (alg !== 'HS256') throw new Error(`Unsupported Skydio-Verification algorithm: ${alg}`);

        let key = JWKCache.get(kid);
        if (!key) {
            const data = await this.skydio(env, `/webhook_validation?key_id=${encodeURIComponent(kid)}`, Type.Object({
                jwk: Type.Object({
                    kid: Type.String(),
                    k: Type.String()
                }, { additionalProperties: true })
            }, { additionalProperties: true }));

            key = Buffer.from(data.jwk.k, 'base64url');
            JWKCache.set(kid, key);
        }

        const expected = crypto.createHmac('sha256', key).update(`${parts[0]}.${parts[1]}`).digest();
        const actual = Buffer.from(parts[2], 'base64url');

        if (actual.length !== expected.length || !crypto.timingSafeEqual(actual, expected)) {
            throw new Error('Skydio-Verification signature mismatch');
        }

        const payload: unknown = JSON.parse(Buffer.from(parts[1], 'base64url').toString());
        const { request_json_body } = this.type(Type.Object({
            request_json_body: Type.Optional(Type.String())
        }, { additionalProperties: true }), payload);

        if (request_json_body) {
            // The framework JSON parser consumes the raw request body so the re-serialized
            // body may not be byte-identical to what Skydio hashed - warn rather than reject
            const hash = crypto.createHash('sha256').update(JSON.stringify(body)).digest('hex');
            if (hash !== request_json_body) {
                console.warn('warn - Skydio-Verification body hash mismatch (signature verified, body was re-serialized)');
            }
        }
    }

    /**
     * Webhook Invocation - Skydio Cloud POSTs event notifications here
     * https://apidocs.skydio.com/reference/webhook_request_format
     */
    static webhooks = async function(schema: Schema, base: ETL): Promise<void> {
        const task = base as Task;

        await schema.get('/:webhookid', {
            name: 'Webhook Healthcheck',
            group: 'Webhook',
            params: Type.Object({ webhookid: Type.String() }),
            res: Type.Object({
                status: Type.Integer(),
                message: Type.String()
            })
        }, (req, res) => {
            res.json({ status: 200, message: `${Task.name} webhook endpoint - POST Skydio events here` });
        });

        await schema.post('/:webhookid', {
            name: 'Skydio Event',
            group: 'Webhook',
            params: Type.Object({ webhookid: Type.String() }),
            body: Type.Any(),
            res: Type.Object({
                status: Type.Integer(),
                message: Type.String()
            })
        }, async (req, res) => {
            try {
                const env = await task.env(InputSchema);

                if (env.WEBHOOK_VERIFY) {
                    await task.verify(env, req.headers['skydio-verification'], req.body);
                }

                const event = task.type(WebhookEvent, req.body);
                const resource = event.data.resource;

                if (env.DEBUG) console.log(JSON.stringify(event));

                if (event.event_type === 'skydio.cloud.event.flight_state') {
                    const flight = task.type(FlightStateResource, resource);

                    await task.alert({
                        icon: flight.type === 'FLIGHT_START' ? 'plane-departure' : 'plane-arrival',
                        title: `Skydio ${flight.vehicle_serial}: ${flight.type === 'FLIGHT_START' ? 'Flight Started' : 'Flight Ended'}`,
                        description: `Flight ${flight.flight_id || 'Unknown'} at ${flight.time || event.event_time || 'Unknown Time'}`
                    });
                } else if (event.event_type === 'skydio.cloud.event.device_alert') {
                    const alert = task.type(DeviceAlertResource, resource);

                    const detail = [];
                    if (alert.mission_result) detail.push(`Mission Result: ${alert.mission_result}`);
                    if (alert.dock_error_type) detail.push(`Dock Error: ${alert.dock_error_type}`);

                    await task.alert({
                        icon: 'alert-triangle',
                        priority: alert.alert_type === 'HUMAN_DETECTED' ? 'red' : 'yellow',
                        title: `Skydio ${alert.device?.id || 'Unknown Device'}: ${alert.alert_type.replace(/_/g, ' ')}`,
                        description: detail.length ? detail.join('\n') : `At ${alert.alert_time || event.event_time || 'Unknown Time'}`
                    });
                } else if (event.event_type === 'skydio.cloud.event.online_status') {
                    const status = task.type(OnlineStatusResource, resource);
                    console.log(`ok - ${status.device?.id || 'Unknown Device'} is now ${status.is_online ? 'online' : 'offline'}`);
                } else if (event.event_type === 'skydio.cloud.event.live_stream_status_changed') {
                    const stream = task.type(LiveStreamResource, resource);

                    const eph = await task.ephemeral(EphemeralSchema);
                    const streams = { ...(eph.streams || {}) };

                    if (stream.live_stream_status === 'LIVE_STREAM_START' && stream.rtsp_url) {
                        streams[stream.vehicle_serial] = {
                            rtsp_url: stream.rtsp_url,
                            stream_type: stream.stream_type || 'color'
                        };
                    } else if (stream.live_stream_status === 'LIVE_STREAM_END') {
                        delete streams[stream.vehicle_serial];
                    }

                    await task.setEphemeral({ ...eph, streams });
                } else if (event.event_type === 'skydio.cloud.event.telemetry_available') {
                    const telemetry = task.type(TelemetryAvailableResource, resource);

                    if (env.IMPORT_FLIGHT_TRACK) {
                        await task.importFlightTrack(env, telemetry.flight_id, telemetry.vehicle_serial);
                    }
                } else {
                    console.log(`ok - ignoring unhandled event type: ${event.event_type}`);
                }

                res.json({ status: 200, message: 'Webhook Processed' });
            } catch (err) {
                console.error(err);
                res.status(400).json({
                    status: 400,
                    message: err instanceof Error ? err.message : String(err)
                });
            }
        });
    }

    /**
     * Schedule Invocation - ensures the Skydio Webhook is registered and streams
     * Live Telemetry for any vehicle currently in flight
     */
    async control(): Promise<void> {
        const env = await this.env(InputSchema);
        const layer = await this.fetchLayer();

        try {
            await this.ensureWebhook(env);
        } catch (err) {
            // Webhook registration failure shouldn't block telemetry tracking
            console.error(err);
        }

        const vehicles = await this.vehicles(env);
        console.log(`ok - found ${vehicles.length} vehicles`);

        const active = vehicles.filter((vehicle) => {
            return ['FLYING', 'PREP'].includes(vehicle.flight_status || '') || vehicle.is_live_streaming;
        });

        if (!active.length) {
            console.log('ok - no vehicles currently in flight');
            return;
        }

        // Leave enough of the Lambda timeout budget to flush the final submission
        const duration = Math.min(env.STREAM_DURATION, Math.max(10, layer.timeout - 15)) * 1000;

        await this.stream(env, active, duration);
    }
}

await local(await Task.init(import.meta.url), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(new Task(import.meta.url), event);
}
