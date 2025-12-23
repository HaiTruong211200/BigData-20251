import { Row, Col, Card, Statistic, Typography } from "antd";
import { ArrowUpOutlined, ArrowDownOutlined, CloudServerOutlined, RocketOutlined } from "@ant-design/icons";

const { Title, Text } = Typography;

// Style chuẩn giống Overview
const cardBody = {
    padding: 14,
    minHeight: 90,
    display: "flex",
    alignItems: "center",
};

const valueStyle = { fontSize: 24, fontWeight: "bold" };

export default function LiveSummaryCards({ data }) {
    const { destStats = [], liveBoard = [] } = data;

    // 1. Tính toán Metrics
    const activeFlights = liveBoard.length;
    
    // Đếm số chuyến dự báo bị Delay/Cancel
    const predictedIssues = liveBoard.filter(
        f => f.prediction === 'DELAYED' || f.prediction === 'CANCELLED'
    ).length;

    // Tổng số máy bay đang phải bay vòng chờ (Holding) từ các sân bay đích
    const totalHolding = destStats.reduce((acc, curr) => acc + (curr.holding_count || 0), 0);

    // Tính trung bình NAS Delay của các sân bay đang theo dõi
    const avgNasDelay = destStats.length 
        ? destStats.reduce((acc, curr) => acc + (curr.avg_nas_delay || 0), 0) / destStats.length 
        : 0;

    return (
        <>
            <Title level={5} style={{ marginBottom: 12 }}>
                System Real-time Pulse
            </Title>
            
            <Row gutter={[20, 20]}>
                {/* Card 1: Active Flights */}
                <Col xs={24} sm={12} md={6}>
                    <Card variant="outlined" styles={{ body: cardBody }}>
                        <Statistic
                            title={<span style={{ fontSize: 12 }}>Active Monitored Flights</span>}
                            value={activeFlights}
                            valueStyle={{ ...valueStyle, color: "#1890ff" }}
                            prefix={<RocketOutlined style={{ fontSize: 18, marginRight: 8 }} />}
                        />
                    </Card>
                </Col>

                {/* Card 2: Predicted Issues */}
                <Col xs={24} sm={12} md={6}>
                    <Card variant="outlined" styles={{ body: cardBody }}>
                        <div style={{ width: "100%", display: "flex", justifyContent: "space-between" }}>
                            <Statistic
                                title={<span style={{ fontSize: 12 }}>Predicted Issues</span>}
                                value={predictedIssues}
                                valueStyle={{ ...valueStyle, color: predictedIssues > 0 ? "#ff4d4f" : "#52c41a" }}
                            />
                            {predictedIssues > 0 && (
                                <Text type="danger" style={{ alignSelf: "center", fontSize: 12 }}>
                                    <ArrowUpOutlined /> High Risk
                                </Text>
                            )}
                        </div>
                    </Card>
                </Col>

                {/* Card 3: Total Holding Stack */}
                <Col xs={24} sm={12} md={6}>
                    <Card variant="outlined" styles={{ body: cardBody }}>
                        <Statistic
                            title={<span style={{ fontSize: 12 }}>Total Aircraft Holding</span>}
                            value={totalHolding}
                            valueStyle={{ ...valueStyle, color: totalHolding > 10 ? "#fa8c16" : "inherit" }}
                            suffix={<span style={{ fontSize: 14, color: "#8c8c8c" }}>aircrafts</span>}
                        />
                    </Card>
                </Col>

                {/* Card 4: Avg System Delay */}
                <Col xs={24} sm={12} md={6}>
                    <Card variant="outlined" styles={{ body: cardBody }}>
                        <div style={{ width: "100%", display: "flex", justifyContent: "space-between" }}>
                            <Statistic
                                title={<span style={{ fontSize: 12 }}>Avg System NAS Delay</span>}
                                value={avgNasDelay}
                                precision={1}
                                valueStyle={valueStyle}
                                suffix={<span style={{ fontSize: 14 }}>min</span>}
                                prefix={<CloudServerOutlined style={{ marginRight: 8 }}/>}
                            />
                            <Text type="secondary" style={{ alignSelf: "center", fontSize: 12 }}>
                                <ArrowDownOutlined /> 2%
                            </Text>
                        </div>
                    </Card>
                </Col>
            </Row>
        </>
    );
}