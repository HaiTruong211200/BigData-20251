import { db } from "../utils/drizzle";
import { flightsDailyStatsTable } from "../schemas/flights_daily_stats";
import { sql } from "drizzle-orm";

export async function getCancellationReasons() {
    const [row] = await db
        .select({
            carrier: sql`SUM(${flightsDailyStatsTable.cancelledCarrierTotal})`,
            weather: sql`SUM(${flightsDailyStatsTable.cancelledWeatherTotal})`,
            nas: sql`SUM(${flightsDailyStatsTable.cancelledNationalAirSystemTotal})`,
            security: sql`SUM(${flightsDailyStatsTable.cancelledSecurityTotal})`,
        })
        .from(flightsDailyStatsTable);

    return {
        carrier: row?.carrier ?? 0,
        weather: row?.weather ?? 0,
        nas: row?.nas ?? 0,
        security: row?.security ?? 0,
    };
}

export async function getMonthlyDelayReasons() {
    const data = await db
        .select({
            flightDate: sql`
                date_trunc('month', ${flightsDailyStatsTable.flightDate})
            `,
            carrier: sql`
                SUM(${flightsDailyStatsTable.carrierDelayTotalMinutes})
            `,
            weather: sql`
                SUM(${flightsDailyStatsTable.weatherDelayTotalMinutes})
            `,
            nas: sql`
                SUM(${flightsDailyStatsTable.nasDelayTotalMinutes})
            `,
            security: sql`
                SUM(${flightsDailyStatsTable.securityDelayTotalMinutes})
            `,
            lateAircraft: sql`
                SUM(${flightsDailyStatsTable.lateAircraftDelayTotalMinutes})
            `,
        })
        .from(flightsDailyStatsTable)
        .groupBy(
            sql`date_trunc('month', ${flightsDailyStatsTable.flightDate})`
        )
        .orderBy(
            sql`date_trunc('month', ${flightsDailyStatsTable.flightDate})`
        );

    return data;
}

export async function getMainKPIsCards(range) {
    const conditions = [];

    if (range?.from) {
        conditions.push(
            sql`${flightsDailyStatsTable.flightDate} >= ${range.from}`
        );
    }

    if (range?.to) {
        conditions.push(
            sql`${flightsDailyStatsTable.flightDate} <= ${range.to}`
        );
    }

    const [row] = await db
        .select({
            totalFlights: sql`SUM(${flightsDailyStatsTable.totalFlights})`,
            onTimeFlights: sql`SUM(${flightsDailyStatsTable.onTimeFlights})`,
            delayedFlights: sql`SUM(${flightsDailyStatsTable.delayedFlights})`,
            cancelledFlights: sql`SUM(${flightsDailyStatsTable.cancelledFlights})`,
        })
        .from(flightsDailyStatsTable)
        .where(
            conditions.length
                ? sql.join(conditions, sql` AND `)
                : undefined
        );

    return {
        totalFlights: row?.totalFlights ?? 0,
        onTimeFlights: row?.onTimeFlights ?? 0,
        delayedFlights: row?.delayedFlights ?? 0,
        cancelledFlights: row?.cancelledFlights ?? 0,
    };
}

export async function getTotalDelayMinutes(range) {
    const conditions = [];

    if (range?.from) {
        conditions.push(
            sql`${flightsDailyStatsTable.flightDate} >= ${range.from}`
        );
    }

    if (range?.to) {
        conditions.push(
            sql`${flightsDailyStatsTable.flightDate} <= ${range.to}`
        );
    }

    const [row] = await db
        .select({
            totalDelayMinutes: sql`
                SUM(${flightsDailyStatsTable.allDelayTotalMinutes})
            `,
        })
        .from(flightsDailyStatsTable)
        .where(
            conditions.length ? sql.join(conditions, sql` AND `) : undefined
        );

    return {
        totalDelayMinutes: row?.totalDelayMinutes ?? 0,
    };
}

export async function getFlightTrendsChart() {

    const data = await db
        .select({
            flightDate: sql`
                date_trunc('month', ${flightsDailyStatsTable.flightDate})
            `,
            onTimeFlights: sql`
                SUM(${flightsDailyStatsTable.onTimeFlights})
            `,
            delayedFlights: sql`
                SUM(${flightsDailyStatsTable.delayedFlights})
            `,
            cancelledFlights: sql`
                SUM(${flightsDailyStatsTable.cancelledFlights})
            `,
        })
        .from(flightsDailyStatsTable)
        .groupBy(
            sql`date_trunc('month', ${flightsDailyStatsTable.flightDate})`
        )
        .orderBy(
            sql`date_trunc('month', ${flightsDailyStatsTable.flightDate})`
        );

    return data;
}
