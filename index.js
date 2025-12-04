import Fastify from 'fastify'
import cors from '@fastify/cors'
import Database from 'better-sqlite3';
import { Parser } from 'json2csv'
import fs from 'fs'
import path from 'path'

// -----------------------------------------
// 1) เปิด DB ด้วย better-sqlite3 เพียงตัวเดียว
// -----------------------------------------
const db = new Database('./hlr_db.db');

// สร้างตาราง (exec เป็น sync ไม่ต้อง await)
db.exec(`
CREATE TABLE IF NOT EXISTS sensor_data_exhaust(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sensor_id TEXT,
    sensor_type TEXT, 
    timestamp INTEGER,
    temp REAL,
    humid REAL,
    co2 REAL
);
`);

db.exec(`
CREATE TABLE IF NOT EXISTS sensor_data_interlock(
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
);
`);

db.exec(`
CREATE TABLE IF NOT EXISTS hlr_adjust_co2_setting(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    create_at INTEGER,
    update_at INTEGER,
    adjust_name TEXT,
    is_active INTEGER, 
    after_exhausts_plus REAL,
    after_exhausts_multiplier REAL,
    after_exhausts_offset REAL,
    before_exhaust_plus REAL,
    before_exhaust_multiplier REAL,
    before_exhaust_offset REAL,
    interlock_4c_plus REAL,
    interlock_4c_multiplier REAL,
    interlock_4c_offset REAL
);
`);

db.exec(`
CREATE TABLE IF NOT EXISTS hlr_adjust_usage_history(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    adjust_name TEXT,
    timestamp INTEGER
);
`);

// -----------------------------------------
// 2) default adjust config
// -----------------------------------------
const defaultAdjust = {
    adjust_name: "default",
    after_exhausts_plus: 52.831276,
    after_exhausts_multiplier: 1.06400140,
    after_exhausts_offset: 0.0,
    before_exhaust_plus: 55.215733,
    before_exhaust_multiplier: 1.072297996,
    before_exhaust_offset: 0.0,
    interlock_4c_plus: 16.238157,
    interlock_4c_multiplier: 1.048766343,
    interlock_4c_offset: 0.0,
};

function fetchSettingOrDefault() {
    const sql = `SELECT * FROM hlr_adjust_co2_setting WHERE is_active = 1 LIMIT 1`;
    const row = db.prepare(sql).get();   // ไม่ส่งพารามิเตอร์
    return row || defaultAdjust;
}
// -----------------------------------------
// 3) Fastify init
// -----------------------------------------
const app = Fastify({
    logger: {
        level: "error"
    }
});

app.register(cors, {
    origin: '*'
});

app.get('/debug', async (request, reply) => {
    return { status: 'ok' }
});

// GET /get/setting_hlr  → ดึง preset ทั้งหมดออกมาแสดง
app.get("/get/setting_hlr", async (request, reply) => {
    try {
        const rows = db
            .prepare(
                `
        SELECT
          id,
          adjust_name,
          is_active,
          create_at,
          update_at,
          after_exhausts_plus,
          after_exhausts_multiplier,
          after_exhausts_offset,
          before_exhaust_plus,
          before_exhaust_multiplier,
          before_exhaust_offset,
          interlock_4c_plus,
          interlock_4c_multiplier,
          interlock_4c_offset
        FROM hlr_adjust_co2_setting
        ORDER BY adjust_name ASC, id ASC
        `
            )
            .all();

        return reply.send({
            status: "ok",
            data: rows,
        });
    } catch (err) {
        request.log?.error(err);
        return reply.code(500).send({
            status: "error",
            message: "internal server error",
        });
    }
});



// เปิด preset ให้ active เพียงตัวเดียว
app.post("/adjust/active", async (request, reply) => {
    try {
        const body = request.body || {};
        const adjust_name = (body.adjust_name || "").trim();

        if (!adjust_name) {
            return reply.code(400).send({
                status: "fail",
                message: "adjust_name is required",
            });
        }

        // ตรวจว่ามี preset นี้จริงไหม
        const existing = db.prepare(
            `SELECT id FROM hlr_adjust_co2_setting WHERE adjust_name = ? LIMIT 1`
        ).get(adjust_name);

        if (!existing) {
            return reply.code(404).send({
                status: "fail",
                message: `adjust_name "${adjust_name}" not found`,
            });
        }

        const now = Date.now();

        // ใช้ transaction ให้การเปลี่ยน active เป็น atomic
        const activateTx = db.transaction((name) => {
            // ปิด active ทั้งหมด
            db.prepare(
                `UPDATE hlr_adjust_co2_setting SET is_active = 0`
            ).run();

            // เปิด active ให้ตัวที่เลือก
            db.prepare(
                `UPDATE hlr_adjust_co2_setting
                 SET is_active = 1, update_at = ?
                 WHERE adjust_name = ?`
            ).run(now, name);
        });

        activateTx(adjust_name);

        const active = db.prepare(
            `SELECT * FROM hlr_adjust_co2_setting WHERE adjust_name = ? LIMIT 1`
        ).get(adjust_name);

        return reply.send({
            status: "ok",
            message: `set "${adjust_name}" as active preset`,
            data: active,
        });

    } catch (err) {
        request.log?.error(err);
        return reply.code(500).send({
            status: "error",
            message: "internal server error",
        });
    }
});


// -----------------------------------------
// 4) POST /update/setting_adjust  (upsert)
// -----------------------------------------
app.post("/update/setting_adjust", async (request, reply) => {
    try {
        const body = request.body || {};
        const adjust_name = (body.adjust_name || "default").trim();

        const payload = {
            adjust_name,
            after_exhausts_plus: body.after_exhausts_plus,
            after_exhausts_multiplier: body.after_exhausts_multiplier,
            after_exhausts_offset: body.after_exhausts_offset,
            before_exhaust_plus: body.before_exhaust_plus,
            before_exhaust_multiplier: body.before_exhaust_multiplier,
            before_exhaust_offset: body.before_exhaust_offset,
            interlock_4c_plus: body.interlock_4c_plus,
            interlock_4c_multiplier: body.interlock_4c_multiplier,
            interlock_4c_offset: body.interlock_4c_offset,
        };

        const now = Date.now();

        const existing = db.prepare(
            `SELECT * FROM hlr_adjust_co2_setting WHERE adjust_name = ? LIMIT 1`
        ).get(adjust_name);

        if (existing) {
            const stmt = db.prepare(`
        UPDATE hlr_adjust_co2_setting SET
          update_at = @update_at,
          after_exhausts_plus       = @after_exhausts_plus,
          after_exhausts_multiplier = @after_exhausts_multiplier,
          after_exhausts_offset     = @after_exhausts_offset,
          before_exhaust_plus       = @before_exhaust_plus,
          before_exhaust_multiplier = @before_exhaust_multiplier,
          before_exhaust_offset     = @before_exhaust_offset,
          interlock_4c_plus         = @interlock_4c_plus,
          interlock_4c_multiplier   = @interlock_4c_multiplier,
          interlock_4c_offset       = @interlock_4c_offset
        WHERE id = @id
      `);

            stmt.run({
                id: existing.id,
                update_at: now,
                after_exhausts_plus:
                    payload.after_exhausts_plus ?? existing.after_exhausts_plus,
                after_exhausts_multiplier:
                    payload.after_exhausts_multiplier ?? existing.after_exhausts_multiplier,
                after_exhausts_offset:
                    payload.after_exhausts_offset ?? existing.after_exhausts_offset,
                before_exhaust_plus:
                    payload.before_exhaust_plus ?? existing.before_exhaust_plus,
                before_exhaust_multiplier:
                    payload.before_exhaust_multiplier ?? existing.before_exhaust_multiplier,
                before_exhaust_offset:
                    payload.before_exhaust_offset ?? existing.before_exhaust_offset,
                interlock_4c_plus:
                    payload.interlock_4c_plus ?? existing.interlock_4c_plus,
                interlock_4c_multiplier:
                    payload.interlock_4c_multiplier ?? existing.interlock_4c_multiplier,
                interlock_4c_offset:
                    payload.interlock_4c_offset ?? existing.interlock_4c_offset,
            });
        } else {
            const stmt = db.prepare(`
        INSERT INTO hlr_adjust_co2_setting(
          create_at, update_at, adjust_name, is_active,
          after_exhausts_plus, after_exhausts_multiplier, after_exhausts_offset,
          before_exhaust_plus, before_exhaust_multiplier, before_exhaust_offset,
          interlock_4c_plus, interlock_4c_multiplier, interlock_4c_offset
        ) VALUES (
          @create_at, @update_at, @adjust_name, @is_active,
          @after_exhausts_plus, @after_exhausts_multiplier, @after_exhausts_offset,
          @before_exhaust_plus, @before_exhaust_multiplier, @before_exhaust_offset,
          @interlock_4c_plus, @interlock_4c_multiplier, @interlock_4c_offset
        )
      `);

            stmt.run({
                create_at: now,
                update_at: now,
                adjust_name,
                is_active: 0, // default: สร้างมาแต่ยังไม่ active

                after_exhausts_plus:
                    payload.after_exhausts_plus ?? defaultAdjust.after_exhausts_plus,
                after_exhausts_multiplier:
                    payload.after_exhausts_multiplier ?? defaultAdjust.after_exhausts_multiplier,
                after_exhausts_offset:
                    payload.after_exhausts_offset ?? defaultAdjust.after_exhausts_offset,
                before_exhaust_plus:
                    payload.before_exhaust_plus ?? defaultAdjust.before_exhaust_plus,
                before_exhaust_multiplier:
                    payload.before_exhaust_multiplier ?? defaultAdjust.before_exhaust_multiplier,
                before_exhaust_offset:
                    payload.before_exhaust_offset ?? defaultAdjust.before_exhaust_offset,
                interlock_4c_plus:
                    payload.interlock_4c_plus ?? defaultAdjust.interlock_4c_plus,
                interlock_4c_multiplier:
                    payload.interlock_4c_multiplier ?? defaultAdjust.interlock_4c_multiplier,
                interlock_4c_offset:
                    payload.interlock_4c_offset ?? defaultAdjust.interlock_4c_offset,
            });
        }

        const saved = db.prepare(
            `SELECT * FROM hlr_adjust_co2_setting WHERE adjust_name = ? LIMIT 1`
        ).get(adjust_name);

        return reply.send({
            status: "ok",
            message: "updated setting_adjust successfully",
            data: saved,
        });

    } catch (err) {
        request.log?.error(err);
        return reply.code(500).send({
            status: "error",
            message: "internal server error",
        });
    }
});



// -----------------------------------------
// 5) POST /adjust/usage  (log history)
// -----------------------------------------
app.post("/adjust/usage", async (request, reply) => {
    try {
        const body = request.body || {};
        const adjust_name = body.adjust_name || "default";
        const now = Date.now();

        const stmt = db.prepare(`
            INSERT INTO hlr_adjust_usage_history (adjust_name, timestamp)
            VALUES (@adjust_name, @timestamp)
        `);

        const info = stmt.run({
            adjust_name,
            timestamp: now,
        });

        return reply.send({
            status: "ok",
            message: "logged adjust usage",
            data: {
                id: info.lastInsertRowid,
                adjust_name,
                timestamp: now,
            },
        });
    } catch (err) {
        request.log?.error(err);
        return reply.code(500).send({
            status: "error",
            message: "internal server error",
        });
    }
});

// -----------------------------------------
// 6) POST /download/tongdy/csv
// -----------------------------------------
app.post("/download/tongdy/csv", async (request, reply) => {
    const { startMs, endMs } = request.body || {};
    if (!startMs || !endMs) return reply.status(400).send("Invalid payload");

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
        GROUP BY minute_th, sensor_type, sensor_id;
    `;
    const rows = db.prepare(query).all(startMs, endMs);

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

    const filename = `sensor_tongdy_avg_1min_${Date.now()}.csv`;
    const filepath = path.join('./', filename);
    fs.writeFileSync(filepath, csv);

    reply.header('Content-Type', 'text/csv');
    reply.header('Content-Disposition', `attachment; filename="${filename}"`);
    return reply.send(fs.createReadStream(filepath));
});

// -----------------------------------------
// 7) POST /download/interlock/csv
// -----------------------------------------
app.post("/download/interlock/csv", async (request, reply) => {
    const { startMs, endMs } = request.body || {};
    if (!startMs || !endMs) return reply.status(400).send("Invalid payload");

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
        GROUP BY minute_th, sensor_type, sensor_id, operation_mode;
    `;
    const rows = db.prepare(query).all(startMs, endMs);

    const parser = new Parser({
        fields: [
            'minute_th',
            'sensor_type',
            'sensor_id',
            'operation_mode',
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

    reply.header('Content-Type', 'text/csv');
    reply.header('Content-Disposition', `attachment; filename="${filename}"`);
    return reply.send(fs.createReadStream(filepath));
});

// -----------------------------------------
// 8) POST /loop/data/iaq
// -----------------------------------------
app.post('/loop/data/iaq', async (request, reply) => {
    const { start, latesttime, rangeSelected } = request.body || {};
    const adjust = fetchSettingOrDefault();

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

        if (latesttime > 0) {
            sql = `
                ${MERGED_CTE}
                SELECT
                    timestamp,
                    sensor_type,
                    sensor_id,
                    operation_mode AS mode,
                    CASE
                        WHEN sensor_id = 'after_scrub'
                            THEN (${adjust.after_exhausts_plus} + (${adjust.after_exhausts_multiplier} * co2)) + ${adjust.after_exhausts_offset}
                        WHEN sensor_id = 'before_scrub'
                            THEN (${adjust.before_exhaust_plus} + (${adjust.before_exhaust_multiplier} * co2)) + ${adjust.before_exhaust_offset}
                        WHEN sensor_id = 'interlock_4c'
                            THEN (${adjust.interlock_4c_plus} + (${adjust.interlock_4c_multiplier} * co2)) + ${adjust.interlock_4c_offset}
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

        } else {
            if (rangeSelected >= 60400000) {
                sql = `
                    ${MERGED_CTE}
                    SELECT
                        (CAST((timestamp + 7*3600*1000) / 180000 AS INTEGER) * 180000) - 7*3600*1000 AS timestamp,
                        sensor_type,
                        sensor_id,
                        operation_mode AS mode,
                        AVG(
                            CASE
                                WHEN sensor_id = 'after_scrub'
                                    THEN (${adjust.after_exhausts_plus} + (${adjust.after_exhausts_multiplier} * co2)) + ${adjust.after_exhausts_offset}
                                WHEN sensor_id = 'before_scrub'
                                    THEN (${adjust.before_exhaust_plus} + (${adjust.before_exhaust_multiplier} * co2)) + ${adjust.before_exhaust_offset}
                                WHEN sensor_id = 'interlock_4c'
                                    THEN (${adjust.interlock_4c_plus} + (${adjust.interlock_4c_multiplier} * co2)) + ${adjust.interlock_4c_offset}
                                ELSE 0
                            END
                        ) AS co2,
                        AVG(humid)      AS humidity,
                        AVG(temp)       AS temp,
                        AVG(fan_speed)  AS fan_speed,
                        AVG(voc)        AS voc
                    FROM merged
                    WHERE timestamp >= ?
                    GROUP BY 1,2,3,4
                    ORDER BY 1 ASC;
                `;
                param = start;

            } else if (rangeSelected >= 43200000) {
                sql = `
                    ${MERGED_CTE}
                    SELECT
                        (CAST((timestamp + 7*3600*1000) / 60000 AS INTEGER) * 60000) - 7*3600*1000 AS timestamp,
                        sensor_type,
                        sensor_id,
                        operation_mode AS mode,
                        AVG(
                            CASE
                                WHEN sensor_id = 'after_scrub'
                                    THEN (${adjust.after_exhausts_plus} + (${adjust.after_exhausts_multiplier} * co2)) + ${adjust.after_exhausts_offset}
                                WHEN sensor_id = 'before_scrub'
                                    THEN (${adjust.before_exhaust_plus} + (${adjust.before_exhaust_multiplier} * co2)) + ${adjust.before_exhaust_offset}
                                WHEN sensor_id = 'interlock_4c'
                                    THEN (${adjust.interlock_4c_plus} + (${adjust.interlock_4c_multiplier} * co2)) + ${adjust.interlock_4c_offset}
                                ELSE 0
                            END
                        ) AS co2,
                        AVG(humid)      AS humidity,
                        AVG(temp)       AS temp,
                        AVG(fan_speed)  AS fan_speed,
                        AVG(voc)        AS voc
                    FROM merged
                    WHERE timestamp >= ?
                    GROUP BY 1,2,3,4
                    ORDER BY 1 ASC;
                `;
                param = start;

            } else {
                sql = `
                    ${MERGED_CTE}
                    SELECT
                        timestamp,
                        sensor_type,
                        sensor_id,
                        operation_mode AS mode,
                        AVG(
                            CASE
                                WHEN sensor_id = 'after_scrub'
                                    THEN (${adjust.after_exhausts_plus} + (${adjust.after_exhausts_multiplier} * co2)) + ${adjust.after_exhausts_offset}
                                WHEN sensor_id = 'before_scrub'
                                    THEN (${adjust.before_exhaust_plus} + (${adjust.before_exhaust_multiplier} * co2)) + ${adjust.before_exhaust_offset}
                                WHEN sensor_id = 'interlock_4c'
                                    THEN (${adjust.interlock_4c_plus} + (${adjust.interlock_4c_multiplier} * co2)) + ${adjust.interlock_4c_offset}
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
    }
});
// -----------------------------------------
// 9) start server
// -----------------------------------------
app.listen({ port: 3011, host: '0.0.0.0' }, (err, address) => {
    if (err) {
        app.log.error(err)
        process.exit(1)
    }
    console.log(`Service hlr-backend run at 3011`)
});
