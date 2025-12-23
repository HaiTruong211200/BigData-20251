import { db } from "../utils/drizzle";
import { originAirportDailyStatsTable } from "../schemas/origin_airport_daily_stats";
import { destinationAirportDailyStatsTable } from "../schemas/destination_airport_daily_stats";
import { airportsTable } from "../schemas/airports";
import { sql, eq, and, gte, lte } from "drizzle-orm";

export async function getAirportScatterStats({ dateRange, airports } = {}) {
    const originConditions = [];
    const destConditions = [];

    // Date filter
    if (dateRange?.from) {
        originConditions.push(
            sql`${originAirportDailyStatsTable.flightDate} >= ${dateRange.from}`
        );
        destConditions.push(
            sql`${destinationAirportDailyStatsTable.flightDate} >= ${dateRange.from}`
        );
    }

    if (dateRange?.to) {
        originConditions.push(
            sql`${originAirportDailyStatsTable.flightDate} <= ${dateRange.to}`
        );
        destConditions.push(
            sql`${destinationAirportDailyStatsTable.flightDate} <= ${dateRange.to}`
        );
    }

    // Airport filter
    if (airports?.length) {
        originConditions.push(
            sql`${originAirportDailyStatsTable.origin} IN (${sql.join(
                airports,
                sql`,`
            )})`
        );
        destConditions.push(
            sql`${destinationAirportDailyStatsTable.dest} IN (${sql.join(
                airports,
                sql`,`
            )})`
        );
    }

    const originStats = db
        .select({
            code: originAirportDailyStatsTable.origin,
            totalDepartures: sql`SUM(${originAirportDailyStatsTable.totalFlights})`
                .mapWith(Number)
                .as("totalDepartures"),
            avgDepDelay: sql`
                COALESCE(
                    SUM(${originAirportDailyStatsTable.allDelayTotalMinutes}) /
                    NULLIF(SUM(${originAirportDailyStatsTable.delayedFlights}), 0),
                    0
                )
            `.mapWith(Number).as("avgDepDelay"),
        })
        .from(originAirportDailyStatsTable)
        .where(originConditions.length ? sql.join(originConditions, sql` AND `) : undefined)
        .groupBy(originAirportDailyStatsTable.origin)
        .as("os");

    const destStats = db
        .select({
            code: destinationAirportDailyStatsTable.dest,
            totalArrivals: sql`SUM(${destinationAirportDailyStatsTable.totalFlights})`
                .mapWith(Number)
                .as("totalArrivals"),
            avgArrDelay: sql`
                COALESCE(
                    SUM(${destinationAirportDailyStatsTable.allDelayTotalMinutes}) /
                    NULLIF(SUM(${destinationAirportDailyStatsTable.delayedFlights}), 0),
                    0
                )
            `.mapWith(Number).as("avgArrDelay"), // ✅ BẮT BUỘC
        })
        .from(destinationAirportDailyStatsTable)
        .where(destConditions.length ? sql.join(destConditions, sql` AND `) : undefined)
        .groupBy(destinationAirportDailyStatsTable.dest)
        .as("ds");

    return await db
        .select({
            airportCode: airportsTable.airportCode,
            airportName: airportsTable.airportName,
            totalDepartures: sql`COALESCE(${originStats.totalDepartures}, 0)`.mapWith(Number),
            avgDepDelay: sql`COALESCE(${originStats.avgDepDelay}, 0)`.mapWith(Number),
            totalArrivals: sql`COALESCE(${destStats.totalArrivals}, 0)`.mapWith(Number),
            avgArrDelay: sql`COALESCE(${destStats.avgArrDelay}, 0)`.mapWith(Number),
        })
        .from(airportsTable)
        .leftJoin(originStats, eq(airportsTable.airportCode, originStats.code))
        .leftJoin(destStats, eq(airportsTable.airportCode, destStats.code))
        .where(
            sql`(${originStats.totalDepartures} > 0 OR ${destStats.totalArrivals} > 0)`
        );
}

// Options airport trong filter
export async function getAirportOptions() {
    return await db
        .select({
            airportCode: airportsTable.airportCode,
            airportName: airportsTable.airportName,
        })
        .from(airportsTable)
        .innerJoin(
            originAirportDailyStatsTable,
            eq(airportsTable.airportCode, originAirportDailyStatsTable.origin)
        )
        .innerJoin(
            destinationAirportDailyStatsTable,
            eq(airportsTable.airportCode, destinationAirportDailyStatsTable.dest)
        )
        .groupBy(
            airportsTable.airportCode,
            airportsTable.airportName
        )
        .orderBy(airportsTable.airportCode);
}

// 2 Chart Row 1
export async function getTopOriginStats({ dateRange, airports } = {}) {

    const conditions = [];

    if (dateRange?.from) {
        conditions.push(
            sql`${originAirportDailyStatsTable.flightDate} >= ${dateRange.from}`
        );
    }

    if (dateRange?.to) {
        conditions.push(
            sql`${originAirportDailyStatsTable.flightDate} <= ${dateRange.to}`
        );
    }

    if (airports?.length) {
        conditions.push(
            sql`${originAirportDailyStatsTable.origin} IN (${sql.join(
                airports,
                sql`,`
            )})`
        );
    }

    const originStats = db
        .select({
            code: originAirportDailyStatsTable.origin,
            totalDepartures: sql`SUM(${originAirportDailyStatsTable.totalFlights})`
                .mapWith(Number)
                .as("totalDepartures"),
            delayedDep: sql`SUM(${originAirportDailyStatsTable.delayedFlights})`
                .mapWith(Number)
                .as("delayedDep"),
            totalDepDelay: sql`SUM(${originAirportDailyStatsTable.allDelayTotalMinutes})`
                .mapWith(Number)
                .as("totalDepDelay"),
        })
        .from(originAirportDailyStatsTable)
        .where(conditions.length ? sql.join(conditions, sql` AND `) : undefined)
        .groupBy(originAirportDailyStatsTable.origin)
        .as("os");

    return await db
        .select({
            airportCode: airportsTable.airportCode,
            airportName: airportsTable.airportName,
            totalDepartures: sql`COALESCE(${originStats.totalDepartures}, 0)`
                .mapWith(Number),
            avgDepDelay: sql`
                COALESCE(
                    ${originStats.totalDepDelay} / NULLIF(${originStats.delayedDep}, 0),
                    0
                )
            `.mapWith(Number),
        })
        .from(airportsTable)
        .innerJoin(
            originStats,
            eq(airportsTable.airportCode, originStats.code)
        )
        .where(sql`${originStats.totalDepartures} > 0`);
}

export async function getAirportStats({ dateRange, airports } = {}) {
    const conditionsOrigin = [];
    const conditionsDest = [];

    if (dateRange?.from) {
        const fromDate = dateRange.from;
        conditionsOrigin.push(gte(originAirportDailyStatsTable.flightDate, fromDate));
        conditionsDest.push(gte(destinationAirportDailyStatsTable.flightDate, fromDate));
    }
    
    if (dateRange?.to) {
        const toDate = dateRange.to;
        conditionsOrigin.push(lte(originAirportDailyStatsTable.flightDate, toDate));
        conditionsDest.push(lte(destinationAirportDailyStatsTable.flightDate, toDate));
    }

    const originStats = db
        .select({
            code: originAirportDailyStatsTable.origin,
            totalDepartures: sql`SUM(${originAirportDailyStatsTable.totalFlights})`
                .mapWith(Number)
                .as("totalDepartures"),
            delayedDep: sql`SUM(${originAirportDailyStatsTable.delayedFlights})`
                .mapWith(Number)
                .as("delayedDep"),
            cancelledDep: sql`SUM(${originAirportDailyStatsTable.cancelledFlights})`
                .mapWith(Number)
                .as("cancelledDep"),
            totalDepDelay: sql`SUM(${originAirportDailyStatsTable.allDelayTotalMinutes})`
                .mapWith(Number)
                .as("totalDepDelay"),
        })
        .from(originAirportDailyStatsTable)
        .where(
            conditionsOrigin.length
                ? sql.join(conditionsOrigin, sql` AND `)
                : undefined
        )
        .groupBy(originAirportDailyStatsTable.origin)
        .as("os");

    const destStats = db
        .select({
            code: destinationAirportDailyStatsTable.dest,
            totalArrivals: sql`SUM(${destinationAirportDailyStatsTable.totalFlights})`
                .mapWith(Number)
                .as("totalArrivals"),
            totalArrDelay: sql`SUM(${destinationAirportDailyStatsTable.allDelayTotalMinutes})`
                .mapWith(Number)
                .as("totalArrDelay"),
            delayedArr: sql`SUM(${destinationAirportDailyStatsTable.delayedFlights})`
                .mapWith(Number)
                .as("delayedArr"),
        })
        .from(destinationAirportDailyStatsTable)
        .where(
            conditionsDest.length
                ? sql.join(conditionsDest, sql` AND `)
                : undefined
        )
        .groupBy(destinationAirportDailyStatsTable.dest)
        .as("ds");

    return await db
        .select({
            airportCode: airportsTable.airportCode,
            airportName: airportsTable.airportName,
            city: airportsTable.cityName,
            totalDepartures: sql`COALESCE(${originStats.totalDepartures}, 0)`.mapWith(Number),
            totalArrivals: sql`COALESCE(${destStats.totalArrivals}, 0)`.mapWith(Number),
            delayedDep: sql`COALESCE(${originStats.delayedDep}, 0)`.mapWith(Number),
            avgDepDelay: sql`COALESCE(${originStats.totalDepDelay} / NULLIF(${originStats.delayedDep}, 0), 0)`.mapWith(Number),
            avgArrDelay: sql`COALESCE(${destStats.totalArrDelay} / NULLIF(${destStats.delayedArr}, 0), 0)`.mapWith(Number),
        })
        .from(airportsTable)
        .leftJoin(originStats, eq(airportsTable.airportCode, originStats.code))
        .leftJoin(destStats, eq(airportsTable.airportCode, destStats.code))
        .where(
            and(
                sql`(${originStats.totalDepartures} > 0 OR ${destStats.totalArrivals} > 0)`,
                airports?.length ? sql`${airportsTable.airportCode} IN (${sql.join(airports, sql`,`)})` : undefined
            )
        )
        .orderBy(sql`${originStats.totalDepartures} DESC`);
}

export async function getAirportPerformanceTableStats({ dateRange, airports } = {}) {
    const originConditions = [];
    const destConditions = [];

    // Date range
    if (dateRange?.from) {
        originConditions.push(
            sql`${originAirportDailyStatsTable.flightDate} >= ${dateRange.from}`
        );
        destConditions.push(
            sql`${destinationAirportDailyStatsTable.flightDate} >= ${dateRange.from}`
        );
    }

    if (dateRange?.to) {
        originConditions.push(
            sql`${originAirportDailyStatsTable.flightDate} <= ${dateRange.to}`
        );
        destConditions.push(
            sql`${destinationAirportDailyStatsTable.flightDate} <= ${dateRange.to}`
        );
    }

    // Airport filter
    if (airports?.length) {
        originConditions.push(
            sql`${originAirportDailyStatsTable.origin} IN (${sql.join(airports, sql`,`)})`
        );
        destConditions.push(
            sql`${destinationAirportDailyStatsTable.dest} IN (${sql.join(airports, sql`,`)})`
        );
    }

    const originStats = db
        .select({
            code: originAirportDailyStatsTable.origin,
            totalDepartures: sql`SUM(${originAirportDailyStatsTable.totalFlights})`
                .mapWith(Number)
                .as("totalDepartures"),
            delayedDep: sql`SUM(${originAirportDailyStatsTable.delayedFlights})`
                .mapWith(Number)
                .as("delayedDep"),
            totalDepDelay: sql`SUM(${originAirportDailyStatsTable.allDelayTotalMinutes})`
                .mapWith(Number)
                .as("totalDepDelay"),
        })
        .from(originAirportDailyStatsTable)
        .where(originConditions.length ? sql.join(originConditions, sql` AND `) : undefined)
        .groupBy(originAirportDailyStatsTable.origin)
        .as("os");

    const destStats = db
        .select({
            code: destinationAirportDailyStatsTable.dest,
            totalArrivals: sql`SUM(${destinationAirportDailyStatsTable.totalFlights})`
                .mapWith(Number)
                .as("totalArrivals"),
            delayedArr: sql`SUM(${destinationAirportDailyStatsTable.delayedFlights})`
                .mapWith(Number)
                .as("delayedArr"),
            totalArrDelay: sql`SUM(${destinationAirportDailyStatsTable.allDelayTotalMinutes})`
                .mapWith(Number)
                .as("totalArrDelay"),
        })
        .from(destinationAirportDailyStatsTable)
        .where(destConditions.length ? sql.join(destConditions, sql` AND `) : undefined)
        .groupBy(destinationAirportDailyStatsTable.dest)
        .as("ds");

    return await db
        .select({
            airportCode: airportsTable.airportCode,
            airportName: airportsTable.airportName,
            city: airportsTable.cityName,

            totalDepartures: sql`COALESCE(${originStats.totalDepartures}, 0)`
                .mapWith(Number),

            avgDepDelay: sql`
                COALESCE(
                    ${originStats.totalDepDelay} /
                    NULLIF(${originStats.delayedDep}, 0),
                    0
                )
            `.mapWith(Number),

            totalArrivals: sql`COALESCE(${destStats.totalArrivals}, 0)`
                .mapWith(Number),

            avgArrDelay: sql`
                COALESCE(
                    ${destStats.totalArrDelay} /
                    NULLIF(${destStats.delayedArr}, 0),
                    0
                )
            `.mapWith(Number),
        })
        .from(airportsTable)
        .leftJoin(originStats, eq(airportsTable.airportCode, originStats.code))
        .leftJoin(destStats, eq(airportsTable.airportCode, destStats.code))
        .where(
            sql`(${originStats.totalDepartures} > 0 OR ${destStats.totalArrivals} > 0)`
        )
        .orderBy(sql`${originStats.totalDepartures} DESC`);
}