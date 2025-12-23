import { Row, Col, Card, Statistic } from "antd";
import Title from "antd/es/typography/Title";
import Text from "antd/es/typography/Text";

const cardBody = {
    padding: 14,
    minHeight: 90,
    display: "flex",
    alignItems: "center",
};

const valueStyle = { fontSize: 20 };

export default function SummaryCards({ summary, delayMetrics }) {
    const {
        totalFlights,
        onTimeFlights,
        delayedFlights,
        cancelledFlights,
    } = summary;

    const { totalDelayMinutes } = delayMetrics;
    const numberFormatter = new Intl.NumberFormat("en-US"); 

    const pct = (count) =>
        totalFlights ? ((count / totalFlights) * 100).toFixed(2) : "0.00";

    return (
        <>
            {/* Title Row 1 */}
            <Title level={5} style={{ marginBottom: 8 }}>
                Main KPIs
            </Title>

            {/* ROW 1 — MAIN KPIs */}
            <Row gutter={[20, 20]} style={{ marginBottom: 20 }}>
                <Col xs={24} sm={12} md={6}>
                    <Card variant="outlined" styles={{ body: cardBody }}>
                        <Statistic
                            title={<span style={{ fontSize: 12 }}>Total Flights</span>}
                            value={totalFlights}
                            formatter={(v) => numberFormatter.format(v ?? 0)}
                            valueStyle={valueStyle}
                        />
                    </Card>
                </Col>

                <Col xs={24} sm={12} md={6}>
                    <Card variant="outlined" styles={{ body: cardBody }}>
                        <div
                            style={{
                                display: "flex",
                                justifyContent: "space-between",
                                width: "100%",
                            }}
                        >
                            <Statistic
                                title={<span style={{ fontSize: 12 }}>On-Time Flights</span>}
                                value={onTimeFlights}
                                formatter={(v) => numberFormatter.format(v ?? 0)}
                                valueStyle={{ ...valueStyle, color: "#3f8600" }}
                            />
                            <Text
                                strong
                                style={{
                                    fontSize: 18,
                                    color: "#3f8600",
                                    alignSelf: "center",
                                }}
                            >
                                {pct(onTimeFlights)}%
                            </Text>
                        </div>
                    </Card>
                </Col>

                <Col xs={24} sm={12} md={6}>
                    <Card variant="outlined" styles={{ body: cardBody }}>
                        <div
                            style={{
                                display: "flex",
                                justifyContent: "space-between",
                                width: "100%",
                            }}
                        >
                            <Statistic
                                title={<span style={{ fontSize: 12 }}>Delayed Flights</span>}
                                value={delayedFlights}
                                formatter={(v) => numberFormatter.format(v ?? 0)}
                                valueStyle={{ ...valueStyle, color: "#fa8c16" }}
                            />
                            <Text
                                strong
                                style={{
                                    fontSize: 18,
                                    color: "#fa8c16",
                                    alignSelf: "center",
                                }}
                            >
                                {pct(delayedFlights)}%
                            </Text>
                        </div>
                    </Card>
                </Col>

                <Col xs={24} sm={12} md={6}>
                    <Card variant="outlined" styles={{ body: cardBody }}>
                        <div
                            style={{
                                display: "flex",
                                justifyContent: "space-between",
                                width: "100%",
                            }}
                        >
                            <Statistic
                                title={<span style={{ fontSize: 12 }}>Cancelled Flights</span>}
                                value={cancelledFlights}
                                formatter={(v) => numberFormatter.format(v ?? 0)}
                                valueStyle={{ ...valueStyle, color: "#cf1322" }}
                            />
                            <Text
                                strong
                                style={{
                                    fontSize: 18,
                                    color: "#cf1322",
                                    alignSelf: "center",
                                }}
                            >
                                {pct(cancelledFlights)}%
                            </Text>
                        </div>
                    </Card>
                </Col>
            </Row>

            {/* Title Row 2 */}
            <Title level={5} style={{ marginBottom: 8 }}>
                Delay Metrics
            </Title>

            {/* ROW 2 — DELAY METRICS */}
            <Row gutter={[20, 20]}>
                <Col xs={24} sm={12} md={8}>
                    <Card variant="outlined" styles={{ body: cardBody }}>
                        <Statistic
                            title={<span style={{ fontSize: 12 }}>Total Delay Time</span>}
                            value={totalDelayMinutes}
                            suffix=" mins"
                            formatter={(v) => v?.toLocaleString("en-US")}
                            valueStyle={valueStyle}
                        />
                    </Card>
                </Col>

                <Col xs={24} sm={12} md={8}>
                    <Card variant="outlined" styles={{ body: cardBody }}>
                        <Statistic
                            title={
                                <span style={{ fontSize: 12 }}>
                                    Avg Delay per Delayed Flight
                                </span>
                            }
                            value={
                                delayedFlights
                                    ? totalDelayMinutes / delayedFlights
                                    : 0
                            }
                            precision={1}
                            suffix=" mins"
                            valueStyle={valueStyle}
                        />
                    </Card>
                </Col>

                <Col xs={24} sm={12} md={8}>
                    <Card variant="outlined" styles={{ body: cardBody }}>
                        <Statistic
                            title={<span style={{ fontSize: 12 }}>Avg Delay per Flight</span>}
                            value={
                                totalFlights
                                    ? totalDelayMinutes / totalFlights
                                    : 0
                            }
                            precision={2}
                            suffix=" mins"
                            valueStyle={valueStyle}
                        />
                    </Card>
                </Col>
            </Row>
        </>
    );
}
