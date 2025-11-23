import React from "react";
import { Card, Segmented } from "antd";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer } from "recharts";

export interface DelayCancelMetrics {
    airline: string;
    delayed: number;
    cancelled: number;
}

interface Props {
    data: DelayCancelMetrics[];
    metric: "delayed" | "cancelled";
    onMetricChange: (value: "delayed" | "cancelled") => void;
}

const DelayCancellationBarChart: React.FC<Props> = ({ data, metric, onMetricChange }) => {
    return (
        <Card
            title="Delay / Cancellation % by Airline"
            extra={
                <Segmented
                    options={[
                        { label: "% Delayed", value: "delayed" },
                        { label: "% Cancelled", value: "cancelled" },
                    ]}
                    value={metric}
                    onChange={(value) => onMetricChange(value as "delayed" | "cancelled")}
                />
            }
            style={{ marginBottom: 24 }}
        >
            <ResponsiveContainer width="100%" height={350}>
                <BarChart data={data}>
                    <XAxis dataKey="airline" />
                    <YAxis />
                    <Tooltip />
                    <Bar dataKey={metric} fill="#1F77B4" />
                </BarChart>
            </ResponsiveContainer>
        </Card>
    );
};

export default DelayCancellationBarChart;
