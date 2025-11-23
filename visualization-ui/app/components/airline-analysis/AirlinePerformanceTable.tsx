import React from "react";
import { Table, Card } from "antd";

export interface AirlinePerformance {
    airline: string;
    totalFlights: number;
    onTime: number;
    delayed: number;
    cancelled: number;
    avgDelay: number;
    severe: number;
}

interface Props {
    data: AirlinePerformance[];
}

const AirlinePerformanceTable: React.FC<Props> = ({ data }) => {
    const columns = [
        { title: "Airline", dataIndex: "airline", key: "airline", sorter: (a: AirlinePerformance, b: AirlinePerformance) => a.airline.localeCompare(b.airline) },
        { title: "Total Flights", dataIndex: "totalFlights", sorter: (a: AirlinePerformance, b: AirlinePerformance) => a.totalFlights - b.totalFlights },
        { title: "% On-Time", dataIndex: "onTime", sorter: (a: AirlinePerformance, b: AirlinePerformance) => a.onTime - b.onTime, render: (v: number) => `${v}%` },
        { title: "% Delayed", dataIndex: "delayed", sorter: (a: AirlinePerformance, b: AirlinePerformance) => a.delayed - b.delayed, render: (v: number) => `${v}%` },
        { title: "% Cancelled", dataIndex: "cancelled", sorter: (a: AirlinePerformance, b: AirlinePerformance) => a.cancelled - b.cancelled, render: (v: number) => `${v}%` },
        { title: "Avg Delay (min)", dataIndex: "avgDelay", sorter: (a: AirlinePerformance, b: AirlinePerformance) => a.avgDelay - b.avgDelay },
        { title: "Severe Delays (>60m)", dataIndex: "severe", sorter: (a: AirlinePerformance, b: AirlinePerformance) => a.severe - b.severe },
    ];

    return (
        <Card title="Airline Performance Leaderboard" style={{ marginBottom: 24 }}>
            <Table 
                columns={columns} 
                dataSource={data} 
                pagination={{ pageSize: 8 }}
                rowKey="airline"
            />
        </Card>
    );
};

export default AirlinePerformanceTable;
