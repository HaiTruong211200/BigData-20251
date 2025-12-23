import { Client } from "cassandra-driver";
import 'dotenv/config'

const client = new Client({
    cloud: {
        secureConnectBundle: process.env.ASTRA_SECURE_BUNDLE_PATH,
    },
    credentials: {
        username: process.env.ASTRA_CLIENT_ID,
        password: process.env.ASTRA_CLIENT_SECRET,
    },
    keyspace: "default_keyspace",
});

let isConnected = false;

export async function getClient() {
    if (!isConnected) {
        await client.connect();
        isConnected = true;
    }
    return client;
}