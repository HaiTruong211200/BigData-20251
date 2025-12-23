import { Card, Select } from "antd";
import {
    ComposedChart,
    Line,
    Area,
    XAxis,
    YAxis,
    CartesianGrid,
    Tooltip,
    Legend,
    ResponsiveContainer,
} from "recharts";
import dayjs from "dayjs";

export default function TaxiOutChart({
    data,
    currentAirport,
    onFilterChange,
    limit,
    onLimitChange,
    airportOptions = [],
}) {

    return (
        <Card
            title={`Origin Taxi-Out History`}
            extra={
                <div style={{ display: "flex", gap: 12, alignItems: "center" }}>
                    <span><b>Origin:</b></span>
                    <Select
                        value={currentAirport}
                        onChange={onFilterChange}
                        style={{ width: 120 }}
                        options={airportOptions}
                    />

                    <span><b>Limit:</b></span>
                    <Select
                        value={limit ?? "all"}
                        onChange={onLimitChange}
                        style={{ width: 120 }}
                        options={[
                            { value: 20, label: "Last 20" },
                            { value: 50, label: "Last 50" },
                            { value: 100, label: "Last 100" },
                            { value: "all", label: "All" },
                        ]}
                    />
                </div>
            }
        >
            <ResponsiveContainer width="100%" height={300}>
                <ComposedChart
                    data={data}
                    margin={{ top: 20, right: 20, bottom: 20, left: 20 }}
                >
                    <CartesianGrid
                        stroke="#f5f5f5"
                        strokeDasharray="3 3"
                        vertical={false}
                    />

                    <XAxis
                        dataKey="window_start"
                        tickFormatter={(t) => dayjs(t).format("HH:mm")}
                        stroke="#8c8c8c"
                    />

                    <YAxis
                        yAxisId="left"
                        label={{
                            value: "Minutes",
                            angle: -90,
                            position: "insideLeft",
                        }}
                        stroke="#1890ff"
                    />

                    <YAxis
                        yAxisId="right"
                        orientation="right"
                        label={{
                            value: "Flights",
                            angle: 90,
                            position: "insideRight",
                        }}
                        stroke="#52c41a"
                    />

                    <Tooltip
                        formatter={(value) => (typeof value === "number" ? value.toFixed(2) : value)}
                        labelFormatter={(t) =>
                            dayjs(t).format("YYYY-MM-DD HH:mm:ss")
                        }
                        contentStyle={{
                            backgroundColor: "rgba(255, 255, 255, 0.9)",
                            borderRadius: 8,
                        }}
                    />
                    <Legend />

                    <Line
                        yAxisId="left"
                        type="monotone"
                        dataKey="avg_taxi_out"
                        name="Avg Taxi Out (Min)"
                        stroke="#1890ff"
                        strokeWidth={3}
                        dot={{ r: 4 }}
                    />

                    <Line
                        yAxisId="right"
                        type="monotone"
                        dataKey="total_departures"
                        name="Total Departures"
                        stroke="#52c41a"
                        strokeWidth={2}
                        strokeDasharray="5 5"
                        dot={false}
                    />
                </ComposedChart>
            </ResponsiveContainer>
        </Card>
    );
}