import { db } from "../utils/drizzle";
import { airlineDailyStatsTable } from "../schemas/airline_daily_stats";
import { airlinesTable } from "../schemas/airlines";
import { sql, eq } from "drizzle-orm";

export async function getAirlinesFromDailyStats() {
    const data = await db
        .select({
            carrierCode: airlineDailyStatsTable.opUniqueCarrier,
            carrierName: airlinesTable.carrierName,
        })
        .from(airlineDailyStatsTable)
        .innerJoin(
            airlinesTable,
            eq(
                airlineDailyStatsTable.opUniqueCarrier,
                airlinesTable.carrierCode
            )
        )
        .groupBy(
            airlineDailyStatsTable.opUniqueCarrier,
            airlinesTable.carrierName
        );

    return data;
}

export async function getAirlineDailyStatsGrouped({ dateRange, airlines } = {}) {
    const conditions = [];

    // Filter theo ngày (dateRange bây giờ là {from: "YYYY-MM-DD", to: "YYYY-MM-DD"})
    if (dateRange?.from) {
        conditions.push(
            sql`${airlineDailyStatsTable.flightDate} >= ${dateRange.from}`
        );
    }

    if (dateRange?.to) {
        conditions.push(
            sql`${airlineDailyStatsTable.flightDate} <= ${dateRange.to}`
        );
    }

    // Filter theo danh sách airlines
    if (airlines?.length) {
        conditions.push(
            sql`${airlineDailyStatsTable.opUniqueCarrier} IN (${sql.join(
                airlines,
                sql`,`
            )})`
        );
    }

    const data = await db
        .select({
            airline: airlineDailyStatsTable.opUniqueCarrier,
            carrierName: airlinesTable.carrierName,

            totalFlights: sql`SUM(${airlineDailyStatsTable.totalFlights})`,
            delayedFlights: sql`SUM(${airlineDailyStatsTable.delayedFlights})`,
            cancelledFlights: sql`SUM(${airlineDailyStatsTable.cancelledFlights})`,
            onTimeFlights: sql`SUM(${airlineDailyStatsTable.onTimeFlights})`,

            carrierCancelTotal: sql`SUM(${airlineDailyStatsTable.carrierCancelTotal})`,
            weatherCancelTotal: sql`SUM(${airlineDailyStatsTable.weatherCancelTotal})`,
            nasCancelTotal: sql`SUM(${airlineDailyStatsTable.nationalAirSystemCancelTotal})`,
            securityCancelTotal: sql`SUM(${airlineDailyStatsTable.securityCancelTotal})`,

            carrierDelayTotalMinutes: sql`SUM(${airlineDailyStatsTable.carrierDelayTotalMinutes})`,
            weatherDelayTotalMinutes: sql`SUM(${airlineDailyStatsTable.weatherDelayTotalMinutes})`,
            nasDelayTotalMinutes: sql`SUM(${airlineDailyStatsTable.nasDelayTotalMinutes})`,
            securityDelayTotalMinutes: sql`SUM(${airlineDailyStatsTable.securityDelayTotalMinutes})`,
            lateAircraftDelayTotalMinutes: sql`SUM(${airlineDailyStatsTable.lateAircraftDelayTotalMinutes})`,
            allDelayTotalMinutes: sql`SUM(${airlineDailyStatsTable.allDelayTotalMinutes})`,
        })
        .from(airlineDailyStatsTable)
        .innerJoin(
            airlinesTable,
            eq(airlineDailyStatsTable.opUniqueCarrier, airlinesTable.carrierCode)
        )
        .where(conditions.length ? sql.join(conditions, sql` AND `) : undefined)
        .groupBy(
            airlineDailyStatsTable.opUniqueCarrier,
            airlinesTable.carrierName
        );

    return data;
}
