import {
    pgTable,
    date,
    text,
    bigint,
    doublePrecision,
} from "drizzle-orm/pg-core";

export const originAirportDailyStatsTable = pgTable(
    "origin_airport_daily_stats",
    {
        flightDate: date("flight_date", { mode: "date" }).notNull(),
        origin: text("origin").notNull(),

        totalFlights: bigint("total_flights", { mode: "number" }).notNull(),
        delayedFlights: bigint("delayed_flights", { mode: "number" }).notNull(),
        cancelledFlights: bigint("cancelled_flights", {
            mode: "number",
        }).notNull(),
        onTimeFlights: bigint("on_time_flights", { mode: "number" }).notNull(),

        carrierCancelTotal: bigint("carrier_cancel_total", {
            mode: "number",
        }).notNull(),
        weatherCancelTotal: bigint("weather_cancel_total", {
            mode: "number",
        }).notNull(),
        nationalAirSystemCancelTotal: bigint(
            "national_air_system_cancel_total",
            { mode: "number" }
        ).notNull(),
        securityCancelTotal: bigint("security_cancel_total", {
            mode: "number",
        }).notNull(),

        carrierDelayTotalMinutes: doublePrecision(
            "carrier_delay_total_minutes"
        ).notNull(),
        weatherDelayTotalMinutes: doublePrecision(
            "weather_delay_total_minutes"
        ).notNull(),
        nasDelayTotalMinutes: doublePrecision(
            "nas_delay_total_minutes"
        ).notNull(),
        securityDelayTotalMinutes: doublePrecision(
            "security_delay_total_minutes"
        ).notNull(),
        lateAircraftDelayTotalMinutes: doublePrecision(
            "late_aircraft_delay_total_minutes"
        ).notNull(),
        allDelayTotalMinutes: doublePrecision(
            "all_delay_total_minutes"
        ).notNull(),
    }
);
