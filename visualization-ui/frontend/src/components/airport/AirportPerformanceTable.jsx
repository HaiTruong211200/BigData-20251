import { Table, Tag, Typography } from "antd";

const { Text } = Typography;

export default function AirportPerformanceTable({ data }) {
    const columns = [
        {
            title: "IATA",
            dataIndex: "airportCode",
            key: "code",
            width: 80,
            align: "center",
            render: (text) => <Text strong>{text}</Text>
        },
        {
            title: "AIRPORT",
            key: "name",
            fixed: 'left',
            width: 250,
            render: (_, record) => (
                <div style={{ display: 'flex', flexDirection: 'column' }}>
                    <Text strong>{record.airportName}</Text>
                    <Text type="secondary" style={{ fontSize: '12px' }}>{record.city}</Text>
                </div>
            ),
        },
        {
            title: "DEPARTURES",
            children: [
                {
                    title: "Total Flights",
                    dataIndex: "totalDepartures",
                    key: "totalDep",
                    align: "center",
                    sorter: (a, b) => a.totalDepartures - b.totalDepartures,
                    render: (v) => v?.toLocaleString()
                },
                {
                    title: "Avg Delay (Min)",
                    dataIndex: "avgDepDelay",
                    key: "avgDepDelay",
                    align: "center",
                    sorter: (a, b) => a.avgDepDelay - b.avgDepDelay,
                    render: (v) => (
                        <Tag 
                            color={v > 25 ? "red" : v > 15 ? "orange" : "green"} 
                            style={{ borderRadius: '4px', minWidth: '50px', textAlign: 'center' }}
                        >
                            {Math.round(v)}m
                        </Tag>
                    )
                }
            ]
        },
        {
            title: "ARRIVALS",
            children: [
                {
                    title: "Total Flights",
                    dataIndex: "totalArrivals",
                    key: "totalArr",
                    align: "center",
                    sorter: (a, b) => a.totalArrivals - b.totalArrivals,
                    render: (v) => v?.toLocaleString()
                },
                {
                    title: "Avg Delay (Min)",
                    dataIndex: "avgArrDelay",
                    key: "avgArrDelay",
                    align: "center",
                    sorter: (a, b) => a.avgArrDelay - b.avgArrDelay,
                    render: (v) => (
                        <Tag 
                            color={v > 25 ? "red" : v > 15 ? "orange" : "green"} 
                            style={{ borderRadius: '4px', minWidth: '50px', textAlign: 'center' }}
                        >
                            {Math.round(v)}m
                        </Tag>
                    )
                }
            ]
        }
    ];

    return (
        <Table 
            dataSource={data} 
            columns={columns} 
            rowKey="airportCode" 
            size="middle"
            bordered
            pagination={{ 
                showTotal: (total) => `Total ${total} airports`
            }}
            scroll={{ x: 1000 }}
        />
    );
}