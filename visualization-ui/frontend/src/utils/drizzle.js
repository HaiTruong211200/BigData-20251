import { neon } from "@neondatabase/serverless";
import { drizzle } from "drizzle-orm/neon-http";

if (!import.meta.env.VITE_DATABASE_URL) {
    throw new Error("DATABASE_URL is not defined");
}

const sql = neon(import.meta.env.VITE_DATABASE_URL, {
    fetchOptions: {
        timeout: 20000,
    },
});

export const db = drizzle(sql, {
    logger: true,
    prepare: false,
});