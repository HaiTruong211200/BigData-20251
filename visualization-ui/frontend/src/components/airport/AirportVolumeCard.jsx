import { Card, Typography } from "antd";

const { Text, Title } = Typography;

export default function AirportVolumeCard({ total = 0 }) {
    return (
        <Card
            title="Volume Overview"
            variant="borderless"
            className="shadow-sm"
        >
            <div
                style={{
                    height: 220,
                    display: "flex",
                    flexDirection: "column",
                    alignItems: "center",
                    justifyContent: "center",
                }}
            >
                <Text type="secondary">Total Airports Analyzed</Text>
                <Title level={2}>{total}</Title>
            </div>
        </Card>
    );
}
