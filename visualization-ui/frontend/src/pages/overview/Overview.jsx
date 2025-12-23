import { useEffect, useState, useMemo } from "react";
import { Row, Col, Typography } from "antd";
import dayjs from "dayjs";
import { useSearchParams } from "react-router-dom";

import FilterBar from "../../common/FilterBar";
import SummaryCards from "../../components/overview/SummaryCards";
import DelayReasonStackedChart from "../../components/overview/DelayReasonStackedChart";
import CancellationReasonChart from "../../components/overview/CancellationReasonChart";
import FlightTrendsChart from "../../components/overview/FlightTrendsChart";
import {
    getMainKPIsCards,
    getFlightTrendsChart,
    getTotalDelayMinutes,
    getMonthlyDelayReasons,
    getCancellationReasons,
} from "../../api/overview";
import Loading from "../../common/Loading";

const { Text } = Typography;

const formatChartData = (data) =>
    data.map((item) => ({
        date: dayjs(item.flightDate).format("MM/YYYY"),
        onTime: Number(item.onTimeFlights),
        delayed: Number(item.delayedFlights),
        cancelled: Number(item.cancelledFlights),
    }));

const formatDelayReasonChartData = (data) =>
    data.map((item) => ({
        time: dayjs(item.flightDate).format("MM/YYYY"),
        weather: Number(item.weather),
        carrier: Number(item.carrier),
        system: Number(item.nas),
        security: Number(item.security),
        lateAircraft: Number(item.lateAircraft),
    }));

const formatCancellationChartData = (data) => [
    { name: "Weather", value: Number(data.weather) },
    { name: "Airline/Carrier", value: Number(data.carrier) },
    { name: "National Air System", value: Number(data.nas) },
    { name: "Security", value: Number(data.security) },
].filter((item) => item.value >= 0);

export default function OverviewTab() {
    const [searchParams] = useSearchParams();
    const [loading, setLoading] = useState(true);
    const [summary, setSummary] = useState(null);
    const [delayMetrics, setDelayMetrics] = useState(null);
    const [charts, setCharts] = useState(null);

    const range = useMemo(() => {
        const from = searchParams.get("from");
        const to = searchParams.get("to");

        if (!from || !to) return null;

        return {
            from: dayjs(from).startOf("day"),
            to: dayjs(to).startOf("day"),   
        };
    }, [searchParams]);

    useEffect(() => {
        let ignore = false;

        async function fetchData() {
            setLoading(true);

            try {
                const [
                    summaryRes,
                    delayRes,
                    flightTrendsRes,
                    delayReasonsRes,
                    cancellationRes,
                ] = await Promise.all([
                    getMainKPIsCards(range ?? undefined),
                    getTotalDelayMinutes(range ?? undefined),
                    getFlightTrendsChart(),
                    getMonthlyDelayReasons(),
                    getCancellationReasons(),
                ]);

                if (ignore) return;

                setSummary(summaryRes);
                setDelayMetrics(delayRes);
                setCharts({
                    flightTrends: formatChartData(flightTrendsRes),
                    delayReasons: formatDelayReasonChartData(delayReasonsRes),
                    cancellation: formatCancellationChartData(cancellationRes),
                });
            } finally {
                if (!ignore) setLoading(false);
            }
        }

        fetchData();

        return () => {
            ignore = true;
        };
    }, [searchParams, range]);

    if (loading) return <Loading />;

    return (
        <>
            <FilterBar enabledFilters={["dateRange"]} />

            {range && (
                <div style={{ marginTop: 8 }}>
                    <Text type="secondary" italic>
                        Data from{" "}
                        <b>{range.from.format("DD/MM/YYYY")}</b>{" "}
                        to{" "}
                        <b>{range.to.format("DD/MM/YYYY")}</b>
                        {" "}— applies to <b>summary cards</b> only.
                    </Text>
                </div>
            )}

            <div style={{ marginTop: 24 }}>
                {summary && delayMetrics && (
                    <SummaryCards
                        summary={summary}
                        delayMetrics={delayMetrics}
                    />
                )}
            </div>

            <div style={{ marginTop: 24 }}>
                <Row gutter={[16, 16]}>
                    <Col xs={24} lg={16}>
                        <DelayReasonStackedChart
                            data={charts.delayReasons}
                        />
                    </Col>
                    <Col xs={24} lg={8}>
                        <CancellationReasonChart
                            data={charts.cancellation}
                        />
                    </Col>
                </Row>
            </div>

            <div style={{ marginTop: 24 }}>
                <FlightTrendsChart
                    data={charts.flightTrends}
                />
            </div>
        </>
    );
}
