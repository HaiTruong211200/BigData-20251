"use client";

import React from "react";
import {
    ResponsiveContainer,
    ScatterChart,
    Scatter,
    XAxis,
    YAxis,
    ZAxis,
    Tooltip,
    Cell,
} from "recharts";

export interface RushHourData {
    day: string;         // Mon, Tue, Wed...
    hour: number;        // 0-23
    delayedFlights: number;
}

interface Props {
    airport: string;
    data: RushHourData[];
}

const days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];

// Tạo màu heatmap từ nhẹ → đậm
const getColor = (value: number) => {
    if (value > 80) return "#B71C1C";
    if (value > 60) return "#D32F2F";
    if (value > 40) return "#EF5350";
    if (value > 20) return "#FF8A80";
    return "#FFCDD2";
};

const AirportRushHourHeatmap: React.FC<Props> = ({ airport, data }) => {
    return (
        <div className="p-4 bg-white shadow rounded-xl mt-6">
            <h2 className="text-lg font-semibold mb-4">
                Airport Rush Hours — {airport}
            </h2>

            <ResponsiveContainer width="100%" height={380}>
                <ScatterChart margin={{ top: 20, right: 20 }}>
                    <XAxis
                        type="number"
                        dataKey="hour"
                        name="Hour"
                        ticks={[0, 4, 8, 12, 16, 20, 23]}
                    />
                    <YAxis
                        type="category"
                        dataKey="day"
                        name="Day"
                        ticks={days}
                        width={70}
                    />
                    <ZAxis dataKey="delayedFlights" range={[50, 500]} />
                    <Tooltip
                        formatter={(value: any) => [`${value} delays`, "Delayed Flights"]}
                    />

                    <Scatter data={data}>
                        {data.map((p, i) => (
                            <Cell key={i} fill={getColor(p.delayedFlights)} />
                        ))}
                    </Scatter>
                </ScatterChart>
            </ResponsiveContainer>
        </div>
    );
};

export default AirportRushHourHeatmap;
