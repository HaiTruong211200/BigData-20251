import { Card, Statistic, Row, Col, Badge, Progress, Typography } from "antd";

const { Text } = Typography;

export default function DestStatsCards({ data = [] }) {
    if (!data || data.length === 0) return null;

    return (
        <Row justify="center" gutter={[20, 20]} style={{ marginBottom: 20 }}>
            {data.map((item) => {
                const nasDelay = Number(item.avg_nas_delay || 0);

                return (
                    <Col
                        key={item.dest}
                        xs={24}
                        sm={12}
                        md={8}
                        lg={4}
                    >
                        <Card size="small" variant="outlined">
                            {/* Header */}
                            <Row justify="space-between" align="middle">
                                <Text strong style={{ fontSize: 18 }}>
                                    {item.dest}
                                </Text>
                                <Badge status="processing" />
                            </Row>

                            {/* Items Holding */}
                            <div style={{ marginTop: 8 }}>
                                <Row justify="space-between" align="middle">
                                    <Text type="secondary" style={{ fontSize: 14 }}>
                                        Items Holding
                                    </Text>

                                    <Text strong style={{ fontSize: 14 }}>
                                        {item.holding_count}
                                    </Text>
                                </Row>
                            </div>

                            {/* NAS Delay */}
                            <div style={{ marginTop: 6 }}>
                                <Row justify="space-between" align="middle">
                                    <Text type="secondary" style={{ fontSize: 14 }}>
                                        NAS Delay
                                    </Text>

                                    <Text strong style={{ fontSize: 14 }}>
                                        {nasDelay.toFixed(2)} mins
                                    </Text>
                                </Row>

                                <Progress
                                    percent={Math.min(nasDelay, 100)}
                                    size="small"
                                    showInfo={false}
                                />
                            </div>
                        </Card>
                    </Col>
                );
            })}
        </Row>
    );
}
