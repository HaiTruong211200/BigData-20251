import {
    pgTable,
    date,
    text,
    bigint,
    doublePrecision,
} from "drizzle-orm/pg-core";

export const destinationAirportDailyStatsTable = pgTable(
    "destination_airport_daily_stats",
    {
        // Kết hợp flightDate và dest làm khóa chính nếu đây là bảng thống kê
        flightDate: date("flight_date", { mode: "date" }).notNull(),
        dest: text("dest").notNull(),

        // Thống kê số lượng (BIGINT)
        totalFlights: bigint("total_flights", { mode: "number" }).notNull(),
        delayedFlights: bigint("delayed_flights", { mode: "number" }).notNull(),
        cancelledFlights: bigint("cancelled_flights", {
            mode: "number",
        }).notNull(),
        onTimeFlights: bigint("on_time_flights", { mode: "number" }).notNull(),

        // Thống kê hủy chuyến
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

        // Thống kê số phút chậm trễ (DOUBLE PRECISION)
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
