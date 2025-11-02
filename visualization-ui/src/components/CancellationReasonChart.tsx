import React from 'react';
import { Card } from 'antd';
import { PieChart, Pie, Tooltip, Cell, Legend, ResponsiveContainer } from 'recharts';

const data = [
  { name: 'Weather', value: 48.85 },
  { name: 'Airline/Carrier', value: 25.26 },
  { name: 'National Air System', value: 15.75 },
  { name: 'Security', value: 0.02 },
];

const COLORS = ['#1890ff', '#52c41a', '#faad14', '#cf1322'];

const CancellationReasonChart: React.FC = () => {
  return (
    <Card title="Total Flights by Cancellation Reason" variant='outlined'>
      <ResponsiveContainer width="100%" height={300}>
        <PieChart>
          <Pie data={data} dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius={100} label>
            {data.map((entry, index) => (
              <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
            ))}
          </Pie>
          <Tooltip />
          <Legend />
        </PieChart>
      </ResponsiveContainer>
    </Card>
  );
};

export default CancellationReasonChart;
