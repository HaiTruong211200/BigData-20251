import { pgTable, date, text, bigint, doublePrecision } from "drizzle-orm/pg-core";

export const airlineDailyStatsTable = pgTable("airline_daily_stats", {
    // Tên cột: flight_date, Kiểu dữ liệu: DATE, Khóa chính
    flightDate: date("flight_date", { mode: 'date' }).primaryKey(),
    
    // Tên cột: op_unique_carrier, Kiểu dữ liệu: TEXT (Thêm cột này từ ảnh)
    opUniqueCarrier: text("op_unique_carrier").notNull(), 
    
    // Các cột thống kê chuyến bay - Kiểu dữ liệu: BIGINT
    totalFlights: bigint("total_flights", { mode: 'bigint' }).notNull(),
    delayedFlights: bigint("delayed_flights", { mode: 'bigint' }).notNull(),
    cancelledFlights: bigint("cancelled_flights", { mode: 'bigint' }).notNull(),
    onTimeFlights: bigint("on_time_flights", { mode: 'bigint' }).notNull(),
    
    // Các cột thống kê hủy chuyến theo lý do - Kiểu dữ liệu: BIGINT
    carrierCancelTotal: bigint("carrier_cancel_total", { mode: 'bigint' }).notNull(),
    weatherCancelTotal: bigint("weather_cancel_total", { mode: 'bigint' }).notNull(),
    nationalAirSystemCancelTotal: bigint("national_air_system_cancel_total", { mode: 'bigint' }).notNull(),
    securityCancelTotal: bigint("security_cancel_total", { mode: 'bigint' }).notNull(),

    // Các cột thống kê chậm trễ theo lý do - Kiểu dữ liệu: DOUBLE PRECISION
    carrierDelayTotalMinutes: doublePrecision("carrier_delay_total_minutes").notNull(),
    weatherDelayTotalMinutes: doublePrecision("weather_delay_total_minutes").notNull(),
    nasDelayTotalMinutes: doublePrecision("nas_delay_total_minutes").notNull(), 
    securityDelayTotalMinutes: doublePrecision("security_delay_total_minutes").notNull(),
    lateAircraftDelayTotalMinutes: doublePrecision("late_aircraft_delay_total_minutes").notNull(),
    
    // Cột tổng thời gian chậm trễ: all_delay_total_minutes
    allDelayTotalMinutes: doublePrecision("all_delay_total_minutes").notNull(),
});