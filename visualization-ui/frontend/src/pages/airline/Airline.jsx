"use client";

import { useEffect, useState, useMemo } from "react";
import { Typography, Spin } from "antd";
import dayjs from "dayjs";
import { useSearchParams } from "react-router-dom";
import FilterBar from "../../common/FilterBar";
import Loading from "../../common/Loading";
import AirlinePerformanceTable from "../../components/airline/AirlinePerformanceTable";
import DelayReasonStackedChart from "../../components/airline/DelayReasonStackedChart";
import { getAirlineDailyStatsGrouped } from "../../api/airline";

const { Text } = Typography;

const pct = (value, total) =>
    total ? Math.round((value / total) * 10000) / 100 : 0;

export default function AirlineAnalysisTab() {
    const [searchParams] = useSearchParams();
    const [loading, setLoading] = useState(true);
    const [leaderboardData, setLeaderboardData] = useState([]);
    const [delayReasonChartData, setDelayReasonChartData] = useState([]);

    const range = useMemo(() => {
        const from = searchParams.get("from");
        const to = searchParams.get("to");

        if (!from || !to) return null;

        return {
            from: dayjs(from).startOf("day"),
            to: dayjs(to).endOf("day"),
        };
    }, [searchParams]);

    const selectedAirlines = useMemo(() => {
            const airlineStr = searchParams.get("airline");
            return airlineStr ? airlineStr.split(",") : [];
        }, [searchParams]);

    useEffect(() => {
        let ignore = false;

        async function fetchData() {
            setLoading(true);
            try {
                const dateRangeParam = range ? {
                    from: range.from.format("YYYY-MM-DD"),
                    to: range.to.format("YYYY-MM-DD")
                } : null;

                const groupedStats = await getAirlineDailyStatsGrouped({
                    dateRange: dateRangeParam,
                    airlines: selectedAirlines,
                });

                if (ignore) return;

                setDelayReasonChartData(
                    groupedStats.map((row) => ({
                        airline: row.carrierName ?? row.airline,
                        carrier: Number(row.carrierDelayTotalMinutes) || 0,
                        weather: Number(row.weatherDelayTotalMinutes) || 0,
                        system: Number(row.nasDelayTotalMinutes) || 0,
                        security: Number(row.securityDelayTotalMinutes) || 0,
                        lateAircraft: Number(row.lateAircraftDelayTotalMinutes) || 0,
                    }))
                );

                setLeaderboardData(
                    groupedStats.map((row) => {
                        const total = Number(row.totalFlights);
                        return {
                            airline: row.airline,
                            carrierName: row.carrierName,
                            totalFlights: total,
                            onTime: pct(Number(row.onTimeFlights), total),
                            delayed: pct(Number(row.delayedFlights), total),
                            cancelled: pct(Number(row.cancelledFlights), total),
                            avgDelay: row.delayedFlights ? Math.round(Number(row.allDelayTotalMinutes) / Number(row.delayedFlights)) : 0,
                            onTimeFlights: row.onTimeFlights,
                            delayedFlights: row.delayedFlights,
                            cancelledFlights: row.cancelledFlights,
                        };
                    })
                );
            } finally {
                if (!ignore) setLoading(false);
            }
        }

        fetchData();
        return () => { ignore = true; };
    }, [range, selectedAirlines]);

    if (loading) return <Loading />;

    return (
        <>
            <Spin spinning={loading}>
                <FilterBar
                    enabledFilters={["dateRange", "airline"]}
                />

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
                    <AirlinePerformanceTable data={leaderboardData} />
                    <DelayReasonStackedChart data={delayReasonChartData} />
                </div>
            </Spin>
        </>
    );
}
