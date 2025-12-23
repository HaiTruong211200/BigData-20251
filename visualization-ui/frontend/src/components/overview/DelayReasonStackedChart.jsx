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

const COLORS = {
    weather: "#1F294A",
    carrier: "#B3283A",
    system: "#D44747",
    security: "#FF5A4A",
    lateAircraft: "#F4A261",
};

const LEGEND_LABELS = {
    weather: "Weather",
    carrier: "Carrier",
    system: "System",
    security: "Security",
    lateAircraft: "Late Aircraft",
};

export default function DelayReasonStackedChart({ data = [] }) {
    const numberFormatter = new Intl.NumberFormat("en-US");

    return (
        <Card title="Analysis of Delay Reasons (mins)" variant="outlined">
            <ResponsiveContainer width="100%" height={350}>
                <BarChart
                    data={data}
                    margin={{ top: 20, right: 30, left: 0, bottom: 10 }}
                >
                    <XAxis dataKey="time" tick={{ fontWeight: "bold" }} />
                    <YAxis 
                        width={80} 
                        tick={{ fill: "#888" }} 
                        tickFormatter={(value) => numberFormatter.format(value)}
                    />
                    <Tooltip
                        formatter={(value, name) => [
                            numberFormatter.format(value),
                            LEGEND_LABELS[name] || name.toUpperCase(),
                        ]}
                    />
                    <Legend
                        formatter={(value) => LEGEND_LABELS[value] || value.toUpperCase()}
                    />

                    <Bar dataKey="weather" stackId="a" fill={COLORS.weather} />
                    <Bar dataKey="carrier" stackId="a" fill={COLORS.carrier} />
                    <Bar dataKey="system" stackId="a" fill={COLORS.system} />
                    <Bar dataKey="security" stackId="a" fill={COLORS.security} />
                    <Bar dataKey="lateAircraft" stackId="a" fill={COLORS.lateAircraft} />
                </BarChart>
            </ResponsiveContainer>
        </Card>
    );
}
