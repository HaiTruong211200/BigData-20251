import { useEffect, useState } from "react";
import { Spin, Row, Col, Typography, Button } from "antd";
import { ReloadOutlined } from "@ant-design/icons";
import { getLiveDashboardData, fetchOriginOptions } from "../../api/live";
import DestStatsCards from "../../components/live/DestStatsCards";
import TaxiOutChart from "../../components/live/TaxiOutChart";
import LiveArrivalTable from "../../components/live/LiveArrivalTable";

const { Text } = Typography;

export default function LiveAnalysisTab() {
    const [loading, setLoading] = useState(true);
    const [data, setData] = useState({
        destStats: [],
        taxiOutHistory: [],
        liveBoard: [],
    });

    const [airportOptions, setAirportOptions] = useState([]);
    
    // Filter cho biểu đồ (Chart)
    const [originFilter, setOriginFilter] = useState("ATL"); 
    const [chartLimit, setChartLimit] = useState(20);

    // Filter cho bảng (Table) - Mặc định xem máy bay đến LAX
    const [tableDest, setTableDest] = useState("LAX");

    const fetchData = async () => {
        setLoading(true);
        try {
            // Gọi hàm tổng hợp từ API
            const result = await getLiveDashboardData(
                originFilter, // chartOrigin
                chartLimit,   // chartLimit
                tableDest     // tableDest
            );
            setData(result);
        } catch (error) {
            console.error("Failed to fetch live data", error);
        } finally {
            setLoading(false);
        }
    };

    // Gọi lại API khi filter thay đổi
    useEffect(() => {
        fetchData();

        const interval = setInterval(fetchData, 30000);
        return () => clearInterval(interval);
    }, [originFilter, chartLimit, tableDest]);

    useEffect(() => {
        fetchOriginOptions().then(setAirportOptions);
    }, []);

    const handleChartFilterChange = (val) => {
        setOriginFilter(val);
    };

    const handleLimitChange = (val) => {
        setChartLimit(val === "all" ? undefined : val);
    };

    return (
        <>
            <Row justify="space-between" align="middle">
                <Text className="font-semibold" style={{ fontSize: 16 }}>Destination Statistics</Text>

                <Button
                    type="primary"
                    icon={<ReloadOutlined spin={loading} />}
                    onClick={fetchData}
                    loading={loading}
                    style={{ padding: 8 }}
                >
                    Refresh Data
                </Button>
            </Row>

            <Spin spinning={loading}>
                {/* 1. Hàng đầu tiên: Các thẻ Cards (Dest Stats) */}
                <div style={{ marginBottom: 24, marginTop: 12 }}>
                    <DestStatsCards data={data.destStats} />
                </div>

                {/* 2. Hàng thứ hai: Biểu đồ Taxi Out & Info */}
                <Row gutter={[24, 24]} style={{ marginBottom: 24 }}>
                    <Col span={24}>
                        <TaxiOutChart
                            data={data.taxiOutHistory}
                            currentAirport={originFilter}
                            onFilterChange={handleChartFilterChange}
                            limit={chartLimit}
                            onLimitChange={handleLimitChange}
                            airportOptions={airportOptions}
                        />
                    </Col>
                </Row>

                {/* 3. Hàng cuối: Bảng Live Board */}
                <LiveArrivalTable
                    data={data.liveBoard}
                    dest={tableDest}
                    onDestChange={setTableDest}
                    airportOptions={airportOptions}
                />
            </Spin>
        </>
    );
}