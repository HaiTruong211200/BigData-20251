"use client";

import { Table, Card, Tooltip  } from "antd";

const AirlinePerformanceTable = ({ data }) => {
    const numberFormatter = new Intl.NumberFormat("en-US");

    
    const columns = [
        {
            title: "Airline",
            dataIndex: "carrierName",
            key: "carrierName",
            sorter: (a, b) => a.carrierName.localeCompare(b.carrierName),
        },
        {
            title: "Total Flights",
            align: "center",
            dataIndex: "totalFlights",
            sorter: (a, b) => a.totalFlights - b.totalFlights,
            render: (value) => numberFormatter.format(value),
        },
        {
            title: "% On-Time",
            align: "center",
            dataIndex: "onTime",
            sorter: (a, b) => a.onTime - b.onTime,
            render: (value, record) => (
                <Tooltip
                    title={`${numberFormatter.format(record.onTimeFlights)} flights`}
                >
                    <span>{value}%</span>
                </Tooltip>
            ),
        },
        {
            title: "% Delayed",
            align: "center",
            dataIndex: "delayed",
            sorter: (a, b) => a.delayed - b.delayed,
            render: (value, record) => (
                <Tooltip
                    title={`${numberFormatter.format(record.delayedFlights)} flights`}
                >
                    <span>{value}%</span>
                </Tooltip>
            ),
        },
        {
            title: "% Cancelled",
            align: "center",
            dataIndex: "cancelled",
            sorter: (a, b) => a.cancelled - b.cancelled,
            render: (value, record) => (
                <Tooltip
                    title={`${numberFormatter.format(record.cancelledFlights)} flights`}
                >
                    <span>{value}%</span>
                </Tooltip>
            ),
        },
        {
            title: "Avg Delay (min)",
            align: "center",
            dataIndex: "avgDelay",
            sorter: (a, b) => a.avgDelay - b.avgDelay,
        },
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
