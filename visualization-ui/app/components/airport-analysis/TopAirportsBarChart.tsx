"use client";

import React from "react";
import {
    BarChart,
    Bar,
    XAxis,
    YAxis,
    Tooltip,
    ResponsiveContainer,
    Cell,
} from "recharts";

export interface AirportPerformance {
    airport: string;
    onTime: number;
    avgDelay: number;
    totalDelayMin: number;
}

interface Props {
    title: string;
    data: AirportPerformance[];
}

const COLORS = ["#4CAF50", "#5CB860", "#70C174", "#8DD68F", "#A7EDAA"];

const TopAirportsBarChart: React.FC<Props> = ({ title, data }) => {
    const sorted = [...data].sort((a, b) => b.onTime - a.onTime).slice(0, 5);

    return (
        <div className="p-4 bg-white shadow rounded-xl">
            <h2 className="text-lg font-semibold mb-4">{title}</h2>

            <ResponsiveContainer width="100%" height={320}>
                <BarChart
                    layout="vertical"
                    data={sorted}
                    margin={{ left: 30, right: 20 }}
                >
                    <YAxis dataKey="airport" type="category" width={50} />
                    <XAxis type="number" />
                    <Tooltip />
                    <Bar dataKey="onTime" radius={[5, 5, 5, 5]}>
                        {sorted.map((e, i) => (
                            <Cell key={i} fill={COLORS[i % COLORS.length]} />
                        ))}
                    </Bar>
                </BarChart>
            </ResponsiveContainer>
        </div>
    );
};

export default TopAirportsBarChart;
