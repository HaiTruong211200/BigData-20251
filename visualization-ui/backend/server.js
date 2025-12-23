import express from "express"
import 'dotenv/config'
import cors from "cors"
import { getClient } from "./db.js"

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

app.get("/api/live-board", async (req, res) => {
    try {
        const { dest } = req.query;  // chỉ lấy dest

        if (!dest) {
            return res.status(400).json({
                error: "'dest' is required",
            });
        }

        const client = await getClient();

        const query = `
            SELECT flight_code, dep_time, dest, origin, prediction, confidence
            FROM live_board
            WHERE dest = ?
        `;

        const result = await client.execute(query, [dest], { prepare: true });

        res.json(result.rows);
    } catch (error) {
        console.error("Error fetching live board:", error);
        res.status(500).json({ error: error.message });
    }
});

// 2. API cho Cards
app.get("/api/dest-stats", async (req, res) => {
    try {
        const { dest } = req.query; // VD: ?dest=LAX, 
        
        if (!dest) {
            return res.status(400).json({ error: "Missing 'dest' parameter" });
        }

        const client = await getClient();
        
        const query = `
            SELECT dest, holding_count, avg_nas_delay
            FROM dest_stats
            WHERE dest = ?
        `;

        const result = await client.execute(query, [dest], { prepare: true });
        
        if (result.rowLength > 0) {
            res.json(result.rows[0]);
        } else {
            res.json({}); // Trả về rỗng nếu không tìm thấy
        }
    } catch (error) {
        console.error("Error fetching dest stats:", error);
        res.status(500).json({ error: error.message });
    }
});

app.get("/api/origin-stats", async (req, res) => {
    try {
        const { origin, limit } = req.query; // ?origin=JFK&limit=20

        if (!origin) {
            return res.status(400).json({ error: "Missing 'origin' parameter" });
        }

        const client = await getClient();

        let query = `
            SELECT window_start, avg_taxi_out, total_departures
            FROM origin_stats
            WHERE origin = ?
        `;

        const params = [origin];

        if (limit && !isNaN(Number(limit))) {
            query += ` LIMIT ?`;
            params.push(Number(limit));
        }

        const result = await client.execute(query, params, { prepare: true });

        const sortedData = result.rows.sort(
            (a, b) => new Date(a.window_start) - new Date(b.window_start)
        );

        res.json(sortedData);
    } catch (error) {
        console.error("Error fetching origin stats:", error);
        res.status(500).json({ error: error.message });
    }
});

// Optiosn ở filter
app.get("/api/origins", async (req, res) => {
    try {
        const client = await getClient();

        const query = `
            SELECT DISTINCT origin
            FROM origin_stats
        `;

        const result = await client.execute(query);

        res.json(
            result.rows.map(r => ({
                value: r.origin,
                label: r.origin,
            }))
        );
    } catch (error) {
        console.error("Error fetching origins:", error);
        res.status(500).json({ error: error.message });
    }
});


app.listen(PORT, () => {});