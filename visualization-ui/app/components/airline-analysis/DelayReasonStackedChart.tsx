import React from "react";
import { Card } from "antd";
import {
    BarChart,
    Bar,
    XAxis,
    YAxis,
    Tooltip,
    Legend,
    ResponsiveContainer,
} from "recharts";

export interface DelayReason {
    airline: string;
    weather: number;
    carrier: number;
    system: number;
    security: number;
}

interface Props {
    data: DelayReason[];
}

const DelayReasonStackedChart: React.FC<Props> = ({ data }) => {
    return (
        <Card title="Delay Reason Breakdown by Airline">
            <ResponsiveContainer width="100%" height={380}>
                <BarChart data={data}>
                    <XAxis dataKey="airline" />
                    <YAxis />
                    <Tooltip />
                    <Legend />

                    <Bar dataKey="weather" stackId="a" fill="#1F294A" />
                    <Bar dataKey="carrier" stackId="a" fill="#B3283A" />
                    <Bar dataKey="system" stackId="a" fill="#D44747" />
                    <Bar dataKey="security" stackId="a" fill="#FF5A4A" />
                </BarChart>
            </ResponsiveContainer>
        </Card>
    );
};

export default DelayReasonStackedChart;
