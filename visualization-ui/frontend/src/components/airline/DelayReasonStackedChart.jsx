"use client";

import React from "react";
import { Card, Empty } from "antd";
import {
    BarChart,
    Bar,
    XAxis,
    YAxis,
    Tooltip,
    Legend,
    ResponsiveContainer,
} from "recharts";
import { DELAY_REASON_CONFIG } from "../../constants/delayReasonConfig";

const DelayReasonStackedChart = ({ data = [] }) => {
    const numberFormatter = new Intl.NumberFormat("en-US");

    if (!data.length) {
        return (
            <Card title="Delay Reason Breakdown by Airline (mins)">
                <Empty description="No data available" />
            </Card>
        );
    }

    const chartHeight = Math.max(data.length * 48, 160);

    return (
        <Card title="Delay Reason Breakdown by Airline (mins)">
            <ResponsiveContainer width="100%" height={chartHeight}>
                <BarChart
                    layout="vertical"
                    data={data}
                    margin={{ top: 16, right: 24, bottom: 16, left: 24 }}
                >
                    <XAxis
                        type="number"
                        domain={[0, "dataMax"]}
                        tickFormatter={(value) =>
                            numberFormatter.format(value)
                        }
                    />
                    <YAxis
                        dataKey="airline"
                        type="category"
                        width={170}
                    />
                    <Tooltip
                        formatter={(value, name) => [
                            numberFormatter.format(value),
                            name,
                        ]}
                    />
                    <Legend />

                    {DELAY_REASON_CONFIG.map((reason) => (
                        <Bar
                            key={reason.key}
                            dataKey={reason.key}
                            name={reason.label}
                            stackId="a"
                            fill={reason.color}
                            barSize={24}
                        />
                    ))}
                </BarChart>
            </ResponsiveContainer>
        </Card>
    );
};

export default DelayReasonStackedChart;
