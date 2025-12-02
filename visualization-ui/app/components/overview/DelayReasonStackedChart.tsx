"use client"

import React from 'react';
import { Card } from 'antd';
import {
    BarChart,
    Bar,
    XAxis,
    YAxis,
    Tooltip,
    Legend,
    ResponsiveContainer,
} from 'recharts';

// Dữ liệu mẫu
const data = [
    { time: '2025-01-01', weather: 250, carrier: 120, system: 180, security: 300 },
    { time: '2025-01-02', weather: 180, carrier: 100, system: 160, security: 200 },
    { time: '2025-01-03', weather: 120, carrier: 80, system: 140, security: 150 },
    { time: '2025-01-04', weather: 200, carrier: 110, system: 170, security: 230 },
];

const COLORS = {
    weather: '#1F294A',     // navy dark
    carrier: '#B3283A',     // dark red
    system: '#D44747',      // red
    security: '#FF5A4A',    // bright red-orange
};

const DelayReasonStackedChart: React.FC = () => {
    return (
        <Card title="Analysis of Delay Reasons" variant="outlined">
            <ResponsiveContainer width="100%" height={350}>
                <BarChart data={data} margin={{ top: 20, right: 30, left: 0, bottom: 10 }}>
                    <XAxis dataKey="time" tick={{ fontWeight: 'bold' }} />
                    <YAxis tick={{ fill: '#888' }} />
                    <Tooltip />
                    <Legend />

                    <Bar dataKey="weather" stackId="a" fill={COLORS.weather} />
                    <Bar dataKey="carrier" stackId="a" fill={COLORS.carrier} />
                    <Bar dataKey="system" stackId="a" fill={COLORS.system} />
                    <Bar dataKey="security" stackId="a" fill={COLORS.security} />
                </BarChart>
            </ResponsiveContainer>
        </Card>
    );
};

export default DelayReasonStackedChart;
