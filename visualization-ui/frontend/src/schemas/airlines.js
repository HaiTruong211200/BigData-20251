import { pgTable, text } from "drizzle-orm/pg-core";

export const airlinesTable = pgTable("airlines", {
    carrierCode: text("carrier_code").primaryKey(),
    carrierName: text("carrier_name").notNull(),
});