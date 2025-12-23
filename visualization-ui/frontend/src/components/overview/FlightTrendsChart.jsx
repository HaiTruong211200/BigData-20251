import React from "react";
import { Card } from "antd";
import {
    AreaChart,
    Area,
    XAxis,
    YAxis,
    CartesianGrid,
    Tooltip,
    Legend,
    ResponsiveContainer,
} from "recharts";

export default function FlightTrendsChart({ data = [] }) {
    const numberFormatter = new Intl.NumberFormat("en-US");

    return (
        <Card title="Flight Trends Over Time" variant="outlined">
            <ResponsiveContainer width="100%" height={400}>
                <AreaChart
                    data={data}
                    margin={{ top: 10, right: 30, left: 0, bottom: 0 }}
                >
                    <defs>
                        <linearGradient id="colorOnTime" x1="0" y1="0" x2="0" y2="1">
                            <stop offset="5%" stopColor="#82ca9d" stopOpacity={0.8} />
                            <stop offset="95%" stopColor="#82ca9d" stopOpacity={0} />
                        </linearGradient>

                        <linearGradient id="colorDelayed" x1="0" y1="0" x2="0" y2="1">
                            <stop offset="5%" stopColor="#ff7300" stopOpacity={0.8} />
                            <stop offset="95%" stopColor="#ff7300" stopOpacity={0} />
                        </linearGradient>

                        <linearGradient
                            id="colorCancelled"
                            x1="0"
                            y1="0"
                            x2="0"
                            y2="1"
                        >
                            <stop offset="5%" stopColor="#cf1322" stopOpacity={0.8} />
                            <stop offset="95%" stopColor="#cf1322" stopOpacity={0} />
                        </linearGradient>
                    </defs>

                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="date" />
                    <YAxis tickFormatter={(value) => numberFormatter.format(value)}/>
                    <Tooltip
                        formatter={(value, name) => [
                            numberFormatter.format(value),
                            name,
                        ]}
                    />
                    <Legend />

                    <Area
                        type="monotone"
                        dataKey="onTime"
                        stackId="1"
                        stroke="#82ca9d"
                        fill="url(#colorOnTime)"
                        name="On Time"
                    />
                    <Area
                        type="monotone"
                        dataKey="delayed"
                        stackId="1"
                        stroke="#ff7300"
                        fill="url(#colorDelayed)"
                        name="Delayed"
                    />
                    <Area
                        type="monotone"
                        dataKey="cancelled"
                        stackId="1"
                        stroke="#cf1322"
                        fill="url(#colorCancelled)"
                        name="Cancelled"
                    />
                </AreaChart>
            </ResponsiveContainer>
        </Card>
    );
}
