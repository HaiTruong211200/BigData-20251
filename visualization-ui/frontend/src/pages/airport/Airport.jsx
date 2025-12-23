"use client";

import { useEffect, useState, useMemo } from "react";
import { Typography, Spin, Row, Col, Card } from "antd";
import { useSearchParams } from "react-router-dom";
import dayjs from "dayjs";

import FilterBar from "../../common/FilterBar";
import Loading from "../../common/Loading";
import AirportPerformanceTable from "../../components/airport/AirportPerformanceTable";
import TopOriginCharts from "../../components/airport/TopOriginCharts";
import AirportScatterGroup from "../../components/airport/AirportScatterGroup";

import { getAirportPerformanceTableStats, getTopOriginStats, getAirportScatterStats } from "../../api/airport";

const { Text } = Typography;

export default function AirportAnalysisTab() {
    const [searchParams] = useSearchParams();
    const [loading, setLoading] = useState(true);

    const [topOriginData, setTopOriginData] = useState([]);
    const [scatterData, setScatterData] = useState([]);
    const [tableData, setTableData] = useState([]);

    const range = useMemo(() => {
        const from = searchParams.get("from");
        const to = searchParams.get("to");

        if (!from || !to) return null;

        return {
            from: dayjs(from).startOf("day"),
            to: dayjs(to).endOf("day"),
        };
    }, [searchParams]);

    const selectedAirports = useMemo(() => {
        const airportStr = searchParams.get("airport");
        return airportStr ? airportStr.split(",") : [];
    }, [searchParams]);

    useEffect(() => {
        let ignore = false;

        async function fetchData() {
            setLoading(true);
            try {
                const dateRangeParam = range
                    ? {
                        from: range.from.format("YYYY-MM-DD"),
                        to: range.to.format("YYYY-MM-DD"),
                    }
                    : null;

                const [topOrigin, scatter, table] = await Promise.all([
                    getTopOriginStats({
                        dateRange: dateRangeParam,
                        airports: selectedAirports,
                    }),
                    getAirportScatterStats({
                        dateRange: dateRangeParam,
                        airports: selectedAirports,
                    }),
                    getAirportPerformanceTableStats({
                        dateRange: dateRangeParam,
                        airports: selectedAirports,
                    }),
                ]);

                if (ignore) return;

                setTopOriginData(topOrigin);
                setScatterData(scatter);
                setTableData(table);
            } finally {
                if (!ignore) setLoading(false);
            }
        }

        fetchData();
        return () => {
            ignore = true;
        };
    }, [range, selectedAirports]);

    if (loading) return <Loading />;

    return (
        <Spin spinning={loading}>
            <FilterBar enabledFilters={["dateRange", "airport"]}/>

            {range && (
                <div style={{ marginTop: 8 }}>
                    <Text type="secondary" italic>
                        Data from{" "}
                        <b>{dayjs(range.from).format("DD/MM/YYYY")}</b> to{" "}
                        <b>{dayjs(range.to).format("DD/MM/YYYY")}</b>
                    </Text>
                </div>
            )}

            <div className="mt-6">
                {/* Row 1: Top 5 Charts */}
                <TopOriginCharts data={topOriginData}/>

                {/* Row 2: Scatter Plot (Tương quan mật độ & trễ) */}
                <AirportScatterGroup data={scatterData} />

                {/* Row 3: Table chi tiết */}
                <div className="mt-6">
                    <Card title="Detailed Airport Statistics">
                        <AirportPerformanceTable data={tableData} />
                    </Card>
                </div>
            </div>
        </Spin>
    );
}
