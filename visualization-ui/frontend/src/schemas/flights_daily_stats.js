import { pgTable, date, bigint, doublePrecision } from "drizzle-orm/pg-core";

export const flightsDailyStatsTable = pgTable("flights_daily_stats", {
    // Tên cột: flight_date, Kiểu dữ liệu: DATE
    flightDate: date("flight_date", { mode: "date" }).primaryKey(),

    // Tên cột: total_flights, Kiểu dữ liệu: BIGINT
    totalFlights: bigint("total_flights", { mode: "bigint" }).notNull(),

    // Tên cột: on_time_flights, Kiểu dữ liệu: BIGINT
    onTimeFlights: bigint("on_time_flights", { mode: "bigint" }).notNull(),

    // Tên cột: delayed_flights, Kiểu dữ liệu: BIGINT
    delayedFlights: bigint("delayed_flights", { mode: "bigint" }).notNull(),

    // Tên cột: cancelled_flights, Kiểu dữ liệu: BIGINT
    cancelledFlights: bigint("cancelled_flights", { mode: "bigint" }).notNull(),

    // Các cột thống kê hủy chuyến theo lý do - Kiểu dữ liệu: BIGINT
    cancelledCarrierTotal: bigint("carrier_cancel_total", { mode: "bigint" }).notNull(),
    cancelledWeatherTotal: bigint("weather_cancel_total", { mode: "bigint" }).notNull(),
    cancelledNationalAirSystemTotal: bigint(
        "national_air_system_cancel_total",
        { mode: "bigint" }
    ).notNull(),
    cancelledSecurityTotal: bigint("security_cancel_total", { mode: "bigint" }).notNull(),

    // Các cột thống kê chậm trễ theo lý do - Kiểu dữ liệu: DOUBLE PRECISION
    carrierDelayTotalMinutes: doublePrecision("carrier_delay_total_minutes").notNull(),
    weatherDelayTotalMinutes: doublePrecision("weather_delay_total_minutes").notNull(),
    nasDelayTotalMinutes: doublePrecision("nas_delay_total_minutes").notNull(),
    securityDelayTotalMinutes: doublePrecision("security_delay_total_minutes").notNull(),
    lateAircraftDelayTotalMinutes: doublePrecision("late_aircraft_delay_total_minutes").notNull(),

    // Tổng thời gian chậm trễ
    allDelayTotalMinutes: doublePrecision("all_delay_total_minutes").notNull(),
});
