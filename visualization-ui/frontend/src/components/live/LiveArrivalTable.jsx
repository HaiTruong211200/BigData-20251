import { Table, Tag, Progress, Typography, Card, Select } from "antd";
import dayjs from "dayjs";

const { Text } = Typography;

export default function LiveArrivalTable({
    data,
    dest,
    onDestChange,
    airportOptions = [],
}) {
    const columns = [
        {
            title: "FLIGHT",
            dataIndex: "flight_code",
            key: "flight",
            align: "center",
            render: (text) => <Text strong>{text}</Text>,
        },
        {
            title: "DEP TIME",
            dataIndex: "dep_time",
            key: "dep_time",
            align: "center",
            render: (t) => dayjs(t).format("HH:mm:ss A"),
        },
        {
            title: "ROUTE",
            key: "route",
            align: "center",
            render: (_, r) => (
                <span>
                    <Text strong>{r.origin}</Text>
                    <span style={{ margin: "0 8px", color: "#bfbfbf" }}>&gt;</span>
                    <Text strong style={{ color: "#1890ff" }}>{r.dest}</Text>
                </span>
            ),
        },
        {
            title: "ML PREDICTION",
            dataIndex: "prediction",
            key: "prediction",
            align: "center",
            render: (status) => {
                let color = "default";
                if (status === "ON_TIME") color = "success";
                if (status === "DELAY_RISK") color = "warning";
                if (status === "CANCELLED") color = "error";

                return (
                    <Tag
                        color={color}
                        style={{ minWidth: 90, textAlign: "center", fontWeight: "bold" }}
                    >
                        {status}
                    </Tag>
                );
            },
        },
        {
            title: "CONFIDENCE",
            dataIndex: "confidence",
            key: "confidence",
            align: "center",
            width: 260,
            render: (val) => {
                const percent = Math.round(val * 100);
                return (
                    <div style={{ width: "100%" }}>
                        <div
                            style={{
                                display: "flex",
                                justifyContent: "flex-end",
                                fontSize: 12,
                                marginBottom: 4,
                                color: "#8c8c8c",
                            }}
                        >
                            {percent}%
                        </div>
                        <Progress percent={percent} showInfo={false} strokeColor="#3b82f6" size="small" />
                    </div>
                );
            },
        }
    ];

    return (
            <Card
                title={
                    <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                        <Text strong>Dest:</Text>
                        <Select
                            allowClear
                            placeholder="Select Destination"
                            value={dest}
                            onChange={onDestChange}
                            style={{ width: 140 }}
                            options={airportOptions}
                        />
                    </div>
                }
                extra={<Text strong>Live Arrival Board & ML Prediction</Text>}
                style={{ marginTop: 24 }}
            >
            <Table
                dataSource={data}
                columns={columns}
                rowKey="flight_code"
                size="middle"
                pagination={{
                    showTotal: (total, range) => `${range[0]}-${range[1]} of ${total} flights`,
                }}
            />
        </Card>
    );
}
