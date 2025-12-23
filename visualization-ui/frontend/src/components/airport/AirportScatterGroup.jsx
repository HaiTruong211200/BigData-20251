import { Row, Col, Card } from "antd";
import {
    ScatterChart,
    Scatter,
    XAxis,
    YAxis,
    CartesianGrid,
    Tooltip,
    ResponsiveContainer,
    ZAxis
} from "recharts";

const CustomScatter = ({ data, xKey, yKey, name, color, xLabel, yLabel }) => {
    const formatValue = (value) => value >= 1000 ? `${value / 1000}k` : value;

    return (
        <ResponsiveContainer width="100%" height={300}>
            <ScatterChart margin={{ top: 20, right: 20, bottom: 20, left: 10 }}>
                <CartesianGrid strokeDasharray="3 3" vertical={false} />
                <XAxis 
                    type="number" 
                    dataKey={xKey} 
                    name={xLabel} 
                    tickFormatter={formatValue}
                    label={{ value: xLabel, position: 'insideBottom', offset: -10, fontSize: 12 }}
                />
                <YAxis 
                    type="number" 
                    dataKey={yKey} 
                    name={yLabel} 
                    unit="m"
                    label={{ value: yLabel, angle: -90, position: 'insideLeft', fontSize: 12 }}
                />
                <ZAxis type="number" range={[50, 200]} />
                <Tooltip 
                    cursor={{ strokeDasharray: '3 3' }}
                    content={({ active, payload }) => {
                        if (active && payload && payload.length) {
                            const d = payload[0].payload;
                            return (
                                <div style={{ backgroundColor: "#fff", padding: "10px", border: "1px solid #ccc", borderRadius: "8px" }}>
                                    <p style={{ margin: 0, fontWeight: 'bold' }}>{d.airportCode}</p>
                                    <p style={{ margin: 0, fontSize: '12px', color: '#666' }}>{d.airportName}</p>
                                    <hr style={{ margin: '5px 0', border: '0.5px solid #eee' }} />
                                    <p style={{ margin: 0 }}>Flights: <b>{d[xKey].toLocaleString()}</b></p>
                                    <p style={{ margin: 0 }}>Avg Delay: <b>{Math.round(d[yKey])} mins</b></p>
                                </div>
                            );
                        }
                        return null;
                    }}
                />
                <Scatter name={name} data={data} fill={color} fillOpacity={0.6} />
            </ScatterChart>
        </ResponsiveContainer>
    );
};

export default function AirportScatterGroup({ data }) {
    return (
        <Row gutter={[16, 16]} style={{ marginTop: 24 }}>
            <Col span={24}>
                <Card 
                    title="Departure Analysis" 
                    subTitle="X-Axis: Flight Count | Y-Axis: Avg Delay Minutes"
                >
                    <CustomScatter 
                        data={data} 
                        xKey="totalDepartures" 
                        yKey="avgDepDelay" 
                        name="Departures" 
                        color="#5c7cfa" 
                        xLabel="Departure Flights"
                        yLabel="Delay (mins)"
                    />
                </Card>
            </Col>
            <Col span={24}>
                <Card 
                    title="Arrival Analysis" 
                    subTitle="X-Axis: Flight Count | Y-Axis: Avg Delay Minutes"
                >
                    <CustomScatter 
                        data={data} 
                        xKey="totalArrivals" 
                        yKey="avgArrDelay" 
                        name="Arrivals" 
                        color="#37b24d" 
                        xLabel="Arrival Flights"
                        yLabel="Delay (mins)"
                    />
                </Card>
            </Col>
        </Row>
    );
}