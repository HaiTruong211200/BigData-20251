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

import { AirportPerformance } from "./TopAirportsBarChart";

interface Props {
    title: string;
    data: AirportPerformance[];
}

const COLORS = ["#E53935", "#EF5350", "#E57373", "#FFA4A2", "#FFCDD2"];

const WorstAirportsBarChart: React.FC<Props> = ({ title, data }) => {
    const sorted = [...data]
        .sort((a, b) => b.totalDelayMin - a.totalDelayMin)
        .slice(0, 5);

    return (
        <div className="p-4 bg-white shadow rounded-xl">
            <h2 className="text-lg font-semibold mb-4">{title}</h2>

            <ResponsiveContainer width="100%" height={320}>
                <BarChart layout="vertical" data={sorted}>
                    <YAxis dataKey="airport" type="category" width={50} />
                    <XAxis type="number" />
                    <Tooltip />
                    <Bar dataKey="totalDelayMin" radius={[5, 5, 5, 5]}>
                        {sorted.map((e, i) => (
                            <Cell key={i} fill={COLORS[i % COLORS.length]} />
                        ))}
                    </Bar>
                </BarChart>
            </ResponsiveContainer>
        </div>
    );
};

export default WorstAirportsBarChart;
