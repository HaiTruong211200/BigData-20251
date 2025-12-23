import { Row, Col, Card, Empty } from "antd";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell, CartesianGrid } from "recharts";

const COLORS = ['#FF4D4F', '#FF7A45', '#FFA940', '#FFC53D', '#FFEC3D'];
const BUSY_COLORS = ['#1890FF', '#40A9FF', '#69C0FF', '#91D5FF', '#BAE7FF'];

export default function TopOriginCharts({ data = [] }) {
    const numberFormatter = new Intl.NumberFormat("en-US");

    // Top 5 Trễ nhất (Origin)
    const worstAirports = [...data]
        .sort((a, b) => b.avgDepDelay - a.avgDepDelay)
        .slice(0, 5);

    // Top 5 Đông nhất (Total Departures)
    const busiestAirports = [...data]
        .sort((a, b) => b.totalDepartures - a.totalDepartures)
        .slice(0, 5);

    const formatXAxis = (tickItem) => {
        if (tickItem >= 1000) return `${(tickItem / 1000).toFixed(1)}k`;
        return tickItem;
    };

    if (!data.length) {
        return (
            <Row gutter={[16, 16]}>
                {[1, 2].map((key) => (
                    <Col span={12} key={key}>
                        <Card>
                            <Empty
                                description="No data available for selected filters"
                            />
                        </Card>
                    </Col>
                ))}
            </Row>
        );
    }

    return (
        <Row gutter={[16, 16]}>
            <Col span={12}>
                <Card
                    title="Top 5 Worst Origin Airports (Avg Delay Min)"
                    variant="borderless"
                    className="shadow-sm"
                >
                    <ResponsiveContainer width="100%" height={280}>
                        <BarChart
                            data={worstAirports}
                            layout="vertical"
                            margin={{ left: 10, right: 30 }}
                        >
                            <CartesianGrid strokeDasharray="3 3" horizontal={false} />
                            <XAxis type="number" tickFormatter={formatXAxis} unit="m" />
                            <YAxis dataKey="airportCode" type="category" width={50} />
                            <Tooltip
                                labelFormatter={(value, payload) =>
                                    `Airport: ${payload[0]?.payload?.airportName || value}`
                                }
                                formatter={(value) => [
                                    `${Math.round(value)} mins`,
                                    "Avg Departure Delay",
                                ]}
                            />
                            <Bar dataKey="avgDepDelay" radius={[0, 4, 4, 0]} barSize={30}>
                                {worstAirports.map((_, index) => (
                                    <Cell key={index} fill={COLORS[index]} />
                                ))}
                            </Bar>
                        </BarChart>
                    </ResponsiveContainer>
                </Card>
            </Col>

            <Col span={12}>
                <Card
                    title="Top 5 Busiest Origin Airports (Total Departures)"
                    variant="borderless"
                    className="shadow-sm"
                >
                    <ResponsiveContainer width="100%" height={280}>
                        <BarChart
                            data={busiestAirports}
                            layout="vertical"
                            margin={{ left: 10, right: 30 }}
                        >
                            <CartesianGrid strokeDasharray="3 3" horizontal={false} />
                            <XAxis type="number" tickFormatter={formatXAxis} />
                            <YAxis dataKey="airportCode" type="category" width={50} />
                            <Tooltip
                                labelFormatter={(value, payload) =>
                                    `Airport: ${payload[0]?.payload?.airportName || value}`
                                }
                                formatter={(value) => [
                                    numberFormatter.format(value),
                                    "Total Departures",
                                ]}
                            />
                            <Bar
                                dataKey="totalDepartures"
                                radius={[0, 4, 4, 0]}
                                barSize={30}
                            >
                                {busiestAirports.map((_, index) => (
                                    <Cell key={index} fill={BUSY_COLORS[index]} />
                                ))}
                            </Bar>
                        </BarChart>
                    </ResponsiveContainer>
                </Card>
            </Col>
        </Row>
    );
}
