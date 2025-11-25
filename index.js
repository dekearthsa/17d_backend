import Fastify from 'fastify'
import cors from '@fastify/cors'
import sqlite3 from 'sqlite3'
import Database from 'better-sqlite3';
import { Parser } from 'json2csv'
import fs from 'fs'
import path from 'path'

// Initialize SQLite database
const dbPromise = new Promise((resolve, reject) => {
    const db = new sqlite3.Database('./hlr_db.db', (err) => {
        if (err) {
            return reject(err)
        }
        resolve(db)
    })
})

dbPromise.then(async (db) => {
    await db.exec(`CREATE TABLE IF NOT EXISTS sensor_data_exhaust(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sensor_id TEXT,
        sensor_type TEXT, 
        timestamp INTEGER,
        temp REAL,
        humid REAL,
        co2 REAL
    )`)
}).catch(err => {
    console.error('Failed to initialize database:', err)
})

dbPromise.then(async (db) => {
    await db.exec(`CREATE TABLE IF NOT EXISTS sensor_data_interlock(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sensor_id TEXT,
        timestamp INTEGER,
        sensor_type TEXT, 
        temp REAL,
        humid REAL,
        co2 REAL,
        operation_mode INTEGER, 
        temp_before_filter REAL,
        fan_speed INTEGER,
        voc REAL
    )`)
}).catch(err => {
    console.error('Failed to initialize database:', err)
})

const app = Fastify({
    logger: {
        level: "error"
    }
});
app.register(cors, {
    origin: '*'
})

app.get('/debug', async (request, reply) => {
    return { status: 'ok' }
});

app.post("/download/tongdy/csv", async (request, reply) => {
    const { startMs, endMs } = request.body;
    if (!startMs) return reply.status(400).send("Invalid payload");
    if (!endMs) return reply.status(400).send("Invalid payload");
    const db = new Database('./hlr_db.db')
    const query = `
                SELECT
                    strftime('%Y-%m-%d %H:%M:00', timestamp/1000, 'unixepoch', '+7 hours') AS minute_th,
                    sensor_type,
                    sensor_id,
                    AVG(temp) AS temp,
                    AVG(humid) AS humid,
                    AVG(co2) AS co2
                FROM sensor_data_exhaust
                WHERE timestamp BETWEEN ? AND ?
                GROUP BY minute_th, sensor_type;
                `
    const rows = db.prepare(query).all(startMs, endMs);
    // console.log(rows);
    const parser = new Parser({
        fields: [
            'minute_th',
            'sensor_type',
            'sensor_id',
            'temp',
            'humid',
            'co2'
        ]
    });
    const csv = parser.parse(rows);

    // --- ตั้งชื่อไฟล์และเขียนชั่วคราว ---
    const filename = `sensor_tongdy_avg_1min_${Date.now()}.csv`;
    const filepath = path.join('./', filename);
    fs.writeFileSync(filepath, csv);

    // --- ส่งออกเป็นไฟล์ให้โหลด ---
    reply.header('Content-Type', 'text/csv');
    reply.header('Content-Disposition', `attachment; filename="${filename}"`);
    return reply.send(fs.createReadStream(filepath));
    // return reply.status(200).send(rows)
})

app.post("/download/interlock/csv", async (request, reply) => {
    const { startMs, endMs } = request.body;
    if (!startMs) return reply.status(400).send("Invalid payload");
    if (!endMs) return reply.status(400).send("Invalid payload");
    const db = new Database('./hlr_db.db')
    const query = `
                SELECT
                    strftime('%Y-%m-%d %H:%M:00', timestamp/1000, 'unixepoch', '+7 hours') AS minute_th,
                    sensor_type,
                    sensor_id,
                    operation_mode,
                    AVG(temp) AS temp,
                    AVG(humid) AS humid,
                    AVG(co2) AS co2,
                    AVG(voc) AS voc,
                    AVG(fan_speed) as fan_speed
                FROM sensor_data_interlock
                WHERE timestamp BETWEEN ? AND ?
                GROUP BY minute_th, sensor_type, operation_mode;
                `
    const rows = db.prepare(query).all(startMs, endMs);
    // console.log(rows);
    const parser = new Parser({
        fields: [
            'minute_th',
            'sensor_type',
            'sensor_id',
            'temp',
            'humid',
            'co2',
            'voc',
            'fan_speed'
        ]
    });
    const csv = parser.parse(rows);

    const filename = `sensor_interlock_avg_1min_${Date.now()}.csv`;
    const filepath = path.join('./', filename);
    fs.writeFileSync(filepath, csv);

    // --- ส่งออกเป็นไฟล์ให้โหลด ---
    reply.header('Content-Type', 'text/csv');
    reply.header('Content-Disposition', `attachment; filename="${filename}"`);
    return reply.send(fs.createReadStream(filepath));
    // return reply.status(200).send(rows)
})

app.post('/loop/data/iaq', async (request, reply) => {
    const { start, latesttime, rangeSelected } = request.body;
    const db = new Database('./hlr_db.db');

    // CTE รวม 2 table ให้เป็นโครงเดียวกัน
    const MERGED_CTE = `
        WITH merged AS (
            SELECT
                timestamp,
                sensor_type,
                sensor_id,
                temp,
                humid,
                co2,
                NULL AS operation_mode,
                NULL AS fan_speed,
                NULL AS voc
            FROM sensor_data_exhaust
            UNION ALL
            SELECT
                timestamp,
                sensor_type,
                sensor_id,
                temp,
                humid,
                co2,
                operation_mode,
                fan_speed,
                voc
            FROM sensor_data_interlock
        )
    `;

    try {
        let sql;
        let param;

        // ─────────────────────────────────────────────
        // 1) SWR: ดึงเฉพาะข้อมูลใหม่หลังจาก latesttime
        // ─────────────────────────────────────────────
        if (latesttime > 0) {
            sql = `
                ${MERGED_CTE}
                SELECT
                    timestamp,
                    sensor_type,
                    sensor_id,
                    operation_mode AS mode,
                    CASE
                        WHEN sensor_id = 'after_exhausts' THEN 52.831276 + (1.06400140 * co2)
                        WHEN sensor_id = 'before_exhaust' THEN 55.215733 + (1.072297996 * co2)
                        WHEN sensor_id = 'interlock_4c' THEN 16.238157 + (1.048766343 * co2)
                        ELSE 0
                    END AS co2,
                    humid AS humidity,
                    temp  AS temp,
                    fan_speed,
                    voc
                FROM merged
                WHERE timestamp >= ?
                ORDER BY timestamp ASC;
            `;
            param = latesttime;

            // ─────────────────────────────────────────────
            // 2) ไม่มี latesttime → ใช้ start + rangeSelected
            // ─────────────────────────────────────────────
        } else {
            if (rangeSelected >= 60400000) {
                // ช่วงยาวมาก → bin 3 นาที
                sql = `
                    ${MERGED_CTE}
                    SELECT
                        (CAST((timestamp + 7*3600*1000) / 180000 AS INTEGER) * 180000) - 7*3600*1000 AS timestamp,
                        sensor_type,
                        sensor_id,
                        operation_mode AS mode,
                        AVG(
                            CASE
                                WHEN sensor_id = 'after_exhausts' THEN 52.831276 + (1.06400140 * co2)
                                WHEN sensor_id = 'before_exhaust' THEN 55.215733 + (1.072297996 * co2)
                                WHEN sensor_id = 'interlock_4c' THEN 16.238157 + (1.048766343 * co2)
                                ELSE 0
                            END
                        ) AS co2,
                        AVG(humid)      AS humidity,
                        AVG(temp)       AS temp,
                        AVG(fan_speed)  AS fan_speed,
                        AVG(voc)        AS voc
                    FROM merged
                    WHERE timestamp >= ?
                    GROUP BY
                        1,  -- timestamp (bin แล้ว)
                        2,  -- sensor_type
                        3,  -- sensor_id
                        4   -- mode
                    ORDER BY 1 ASC;
                `;
                param = start;

            } else if (rangeSelected >= 43200000) {
                // 12–16 ชั่วโมง → bin 1 นาที
                sql = `
                    ${MERGED_CTE}
                    SELECT
                        (CAST((timestamp + 7*3600*1000) / 60000 AS INTEGER) * 60000) - 7*3600*1000 AS timestamp,
                        sensor_type,
                        sensor_id,
                        operation_mode AS mode,
                        AVG(
                            CASE
                                WHEN sensor_id = 'after_exhausts' THEN 52.831276 + (1.06400140 * co2)
                                WHEN sensor_id = 'before_exhaust' THEN 55.215733 + (1.072297996 * co2)
                                WHEN sensor_id = 'interlock_4c' THEN 16.238157 + (1.048766343 * co2)
                                ELSE 0
                            END
                        ) AS co2,
                        AVG(humid)      AS humidity,
                        AVG(temp)       AS temp,
                        AVG(fan_speed)  AS fan_speed,
                        AVG(voc)        AS voc
                    FROM merged
                    WHERE timestamp >= ?
                    GROUP BY
                        1,  -- timestamp (bin แล้ว)
                        2,  -- sensor_type
                        3,  -- sensor_id
                        4   -- mode
                    ORDER BY 1 ASC;
                `;
                param = start;

            } else {
                // ช่วงสั้นกว่า 12 ชั่วโมง → ไม่ bin เวลา แต่ average ที่ timestamp เดียวกัน
                sql = `
                    ${MERGED_CTE}
                    SELECT
                        timestamp,
                        sensor_type,
                        sensor_id,
                        operation_mode AS mode,
                        AVG(
                            CASE
                                WHEN sensor_id = 'after_exhausts' THEN 52.831276 + (1.06400140 * co2)
                                WHEN sensor_id = 'before_exhaust' THEN 55.215733 + (1.072297996 * co2)
                                WHEN sensor_id = 'interlock_4c' THEN 16.238157 + (1.048766343 * co2)
                                ELSE 0
                            END
                        ) AS co2,
                        AVG(humid)      AS humidity,
                        AVG(temp)       AS temp,
                        AVG(fan_speed)  AS fan_speed,
                        AVG(voc)        AS voc
                    FROM merged
                    WHERE timestamp >= ?
                    GROUP BY
                        timestamp,
                        sensor_type,
                        sensor_id,
                        operation_mode
                    ORDER BY timestamp ASC;
                `;
                param = start;
            }
        }

        const rows = db.prepare(sql).all(param);
        return reply.send(rows);
    } catch (err) {
        console.error('Error in /loop/data/iaq:', err);
        reply.status(500).send({ error: 'Internal server error' });
    } finally {
        db.close();
    }
});


app.listen({ port: 3011, host: '0.0.0.0' }, (err, address) => {
    if (err) {
        app.log.error(err)
        process.exit(1)
    }
    console.log(`Service hlr-backend run at 3011`)
});