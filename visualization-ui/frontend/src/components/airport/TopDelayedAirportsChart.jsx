import { Card, Row, Col } from "antd";
import {
    BarChart,
    Bar,
    XAxis,
    YAxis,
    Tooltip,
    ResponsiveContainer,
    CartesianGrid,
    Cell,
} from "recharts";
import { useMemo } from "react";

const COLORS = ["#6366f1", "#8b5cf6", "#ec4899", "#f43f5e", "#f59e0b"];

export default function TopDelayedAirportsChart({ data }) {
    const topDelayed = useMemo(() => {
        return [...data]
            .sort((a, b) => b.avgDepDelay - a.avgDepDelay)
            .slice(0, 5);
    }, [data]);

    return (
        <Row gutter={[16, 16]}>
            <Col span={12}>
                <Card
                    title="Top 5 Most Delayed Origin Airports (Avg Minutes)"
                    variant="borderless"
                    className="shadow-sm"
                >
                    <ResponsiveContainer width="100%" height={300}>
                        <BarChart
                            data={topDelayed}
                            layout="vertical"
                            margin={{ left: 30, right: 30 }}
                        >
                            <CartesianGrid
                                strokeDasharray="3 3"
                                horizontal={false}
                            />
                            <XAxis type="number" hide />
                            <YAxis
                                dataKey="airportCode"
                                type="category"
                                width={60}
                            />
                            <Tooltip cursor={{ fill: "transparent" }} />
                            <Bar
                                dataKey="avgDepDelay"
                                barSize={28}
                                radius={[0, 4, 4, 0]}
                            >
                                {topDelayed.map((_, index) => (
                                    <Cell
                                        key={index}
                                        fill={
                                            COLORS[index % COLORS.length]
                                        }
                                    />
                                ))}
                            </Bar>
                        </BarChart>
                    </ResponsiveContainer>
                </Card>
            </Col>
        </Row>
    );
}
