import React from 'react';
import { Card } from 'antd';
import {
    AreaChart,
    Area,
    XAxis,
    YAxis,
    CartesianGrid,
    Tooltip,
    Legend,
    ResponsiveContainer,
} from 'recharts';

// Ví dụ dữ liệu theo ngày
const data = [
    { date: '01/11', onTime: 190, delayed: 80, cancelled: 10 },
    { date: '02/11', onTime: 200, delayed: 60, cancelled: 15 },
    { date: '03/11', onTime: 180, delayed: 90, cancelled: 5 },
    { date: '04/11', onTime: 210, delayed: 70, cancelled: 20 },
    { date: '05/11', onTime: 220, delayed: 50, cancelled: 10 },
];

const FlightTrendsChart: React.FC = () => {
    return (
        <Card title="Flight Trends Over Time" variant="outlined">
            <ResponsiveContainer width="100%" height={400}>
                <AreaChart data={data} margin={{ top: 10, right: 30, left: 0, bottom: 0 }}>
                    <defs>
                        <linearGradient id="colorOnTime" x1="0" y1="0" x2="0" y2="1">
                            <stop offset="5%" stopColor="#82ca9d" stopOpacity={0.8} />
                            <stop offset="95%" stopColor="#82ca9d" stopOpacity={0} />
                        </linearGradient>
                        <linearGradient id="colorDelayed" x1="0" y1="0" x2="0" y2="1">
                            <stop offset="5%" stopColor="#ff7300" stopOpacity={0.8} />
                            <stop offset="95%" stopColor="#ff7300" stopOpacity={0} />
                        </linearGradient>
                        <linearGradient id="colorCancelled" x1="0" y1="0" x2="0" y2="1">
                            <stop offset="5%" stopColor="#cf1322" stopOpacity={0.8} />
                            <stop offset="95%" stopColor="#cf1322" stopOpacity={0} />
                        </linearGradient>
                    </defs>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="date" />
                    <YAxis />
                    <Tooltip />
                    <Legend />
                    {/* Series xếp chồng */}
                    <Area type="monotone" dataKey="onTime" stackId="1" stroke="#82ca9d" fill="url(#colorOnTime)" name="On Time" />
                    <Area type="monotone" dataKey="delayed" stackId="1" stroke="#ff7300" fill="url(#colorDelayed)" name="Delayed" />
                    <Area type="monotone" dataKey="cancelled" stackId="1" stroke="#cf1322" fill="url(#colorCancelled)" name="Cancelled" />
                </AreaChart>
            </ResponsiveContainer>
        </Card>
    );
};

export default FlightTrendsChart;
