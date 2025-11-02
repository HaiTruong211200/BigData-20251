import React from 'react';
import { Card } from 'antd';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';

const data = [
  { airline: 'WN', total: 2.8, onTime: 1.9, delayed: 0.8, cancelled: 0.1 },
  { airline: 'DL', total: 2.3, onTime: 1.7, delayed: 0.5, cancelled: 0.1 },
  { airline: 'AA', total: 2.1, onTime: 1.5, delayed: 0.5, cancelled: 0.1 },
];

const FlightAnalysisChart: React.FC = () => {
  return (
    <Card title="Flight Analysis by Airline" variant='outlined'>
      <ResponsiveContainer width="100%" height={300}>
        <LineChart data={data}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="airline" />
          <YAxis />
          <Tooltip />
          <Legend />
          <Line type="monotone" dataKey="total" stroke="#8884d8" name="Total Flights" />
          <Line type="monotone" dataKey="onTime" stroke="#82ca9d" name="On Time" />
          <Line type="monotone" dataKey="delayed" stroke="#ff7300" name="Delayed" />
          <Line type="monotone" dataKey="cancelled" stroke="#cf1322" name="Cancelled" />
        </LineChart>
      </ResponsiveContainer>
    </Card>
  );
};

export default FlightAnalysisChart;
