import { Card } from "antd";
import {
    PieChart,
    Pie,
    Tooltip,
    Cell,
    Legend,
    ResponsiveContainer,
} from "recharts";

const COLORS = ["#1890ff", "#52c41a", "#faad14", "#cf1322"];
const RADIAN = Math.PI / 180;

export default function CancellationReasonChart({ data = [] }) {
    const total = data.reduce((sum, item) => sum + (item.value || 0), 0);

    const renderCustomizedLabel = ({
        cx,
        cy,
        midAngle,
        outerRadius,
        value,
    }) => {
        if (!total) return null;

        const percent = (value / total) * 100;
        if (percent <= 0) return null;

        const radius = outerRadius * 1.1; 
        const x = cx + radius * Math.cos(-midAngle * RADIAN);
        const y = cy + radius * Math.sin(-midAngle * RADIAN);
        return (
            <text
                x={x}
                y={y}
                fill="#000"
                textAnchor={x > cx ? "start" : "end"}
                dominantBaseline="central"
                fontWeight="bold"
            >
                {`${percent.toFixed(1)}%`}
            </text>
        );
    };

    return (
        <Card title="Total Flights by Cancellation Reason" variant="outlined">
            <ResponsiveContainer width="100%" height={350}>
                <PieChart>
                    <Pie
                        data={data}
                        dataKey="value"
                        nameKey="name"
                        cx="50%"
                        cy="50%"
                        innerRadius={70}
                        outerRadius={110}
                        labelLine={false}
                        label={renderCustomizedLabel}
                        paddingAngle={1}
                    >
                        {data.map((_, index) => (
                            <Cell
                                key={`cell-${index}`}
                                fill={COLORS[index % COLORS.length]}
                            />
                        ))}
                    </Pie>

                    <Tooltip
                        formatter={(value, name) => {
                            const numericValue = Number(value || 0);
                            const percent = total
                                ? ((numericValue / total) * 100).toFixed(2)
                                : 0;
                            return [
                                `${numericValue.toLocaleString()} (${percent}%)`,
                                name,
                            ];
                        }}
                    />
                    <Legend />
                </PieChart>
            </ResponsiveContainer>
        </Card>
    );
}
