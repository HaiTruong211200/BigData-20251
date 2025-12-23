import { pgTable, text } from "drizzle-orm/pg-core";

export const airportsTable = pgTable("airports", {
    airportCode: text("airport_code").primaryKey(),
    airportName: text("airport_name").notNull(),
    cityName: text("city_name").notNull(),
    stateOrCountry: text("state_or_country").notNull(),
});
