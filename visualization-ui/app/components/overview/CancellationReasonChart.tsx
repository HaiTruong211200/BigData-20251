import React from 'react';
import { Card } from 'antd';
import { PieChart, Pie, Tooltip, Cell, Legend, ResponsiveContainer } from 'recharts';

const data = [
    { name: 'Weather', value: 45.85 },
    { name: 'Airline/Carrier', value: 25.26 },
    { name: 'National Air System', value: 15.75 },
    { name: 'Security', value: 3.02 },
];

const COLORS = ['#1890ff', '#52c41a', '#faad14', '#cf1322'];

const RADIAN = Math.PI / 180;

const total = data.reduce((sum, item) => sum + item.value, 0);

const renderCustomizedLabel = ({
    cx,
    cy,
    midAngle,
    innerRadius,
    outerRadius,
    value,
}: any) => {

    const percent = (value / total) * 100;

    if (percent < 1) return null; // tránh label quá nhỏ

    const radius = innerRadius + (outerRadius - innerRadius) * 0.5;
    const x = cx + radius * Math.cos(-midAngle * RADIAN);
    const y = cy + radius * Math.sin(-midAngle * RADIAN);

    return (
        <text
            x={x}
            y={y}
            fill="#fff"
            textAnchor={x > cx ? 'start' : 'end'}
            dominantBaseline="central"
            fontWeight="bold"
        >
            {`${percent.toFixed(0)}%`}
        </text>
    );
};

const CancellationReasonChart: React.FC = () => {
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
                        {data.map((entry, index) => (
                            <Cell key={`cell-${index}`} fill={COLORS[index]} />
                        ))}
                    </Pie>

                    <Tooltip
                        formatter={(value: any, name: string) => {
                            const percent = ((value / total) * 100).toFixed(2);
                            return `${percent}%`;
                        }}
                    />
                    <Legend />
                </PieChart>
            </ResponsiveContainer>
        </Card>
    );
};

export default CancellationReasonChart;
