"use client"

import React, { useState } from "react";
import { Row, Col } from "antd";
import FilterBar from "../components/FilterBar";

import AirlinePerformanceTable, { AirlinePerformance } from "@/app/components/airline-analysis/AirlinePerformanceTable"
import DelayCancellationBarChart, { DelayCancelMetrics } from "@/app/components/airline-analysis/DelayCancellationBarChart";
import DelayReasonStackedChart, { DelayReason } from "@/app/components/airline-analysis/DelayReasonStackedChart";

const AirlineAnalysisTab: React.FC = () => {
    const [metric, setMetric] = useState<"delayed" | "cancelled">("delayed");

    const leaderboardData: AirlinePerformance[] = [
        { airline: "Delta", totalFlights: 12000, onTime: 88, delayed: 10, cancelled: 2, avgDelay: 12, severe: 45 },
        { airline: "United", totalFlights: 9800, onTime: 85, delayed: 12, cancelled: 3, avgDelay: 15, severe: 62 },
    ];

    const delayCancelData: DelayCancelMetrics[] = [
        { airline: "Delta", delayed: 10, cancelled: 2 },
        { airline: "United", delayed: 12, cancelled: 3 },
    ];

    const delayReasonData: DelayReason[] = [
        { airline: "Delta", weather: 30, carrier: 20, system: 10, security: 5 },
        { airline: "United", weather: 20, carrier: 25, system: 12, security: 7 },
    ];

    return (
        <>
            <FilterBar enabledFilters={["dateRange", "airline"]} />
            <div className="mt-6">
                {/* Chart 1: Bảng xếp hạng */}
                <AirlinePerformanceTable data={leaderboardData} />

                {/* Chart 2 và Chart 3 cùng một hàng */}
                <Row gutter={24}>
                    <Col span={12}>
                        <DelayCancellationBarChart
                            data={delayCancelData}
                            metric={metric}
                            onMetricChange={setMetric}
                        />
                    </Col>

                    <Col span={12}>
                        <DelayReasonStackedChart data={delayReasonData} />
                    </Col>
                </Row>
            </div>
        </>
    );
};

export default AirlineAnalysisTab;
