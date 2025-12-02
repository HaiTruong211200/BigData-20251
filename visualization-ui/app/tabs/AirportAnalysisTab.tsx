"use client";

import React, { useState } from "react";
import { Row, Col } from "antd";

import FilterBar from "../components/FilterBar";

import TopAirportsBarChart, { AirportPerformance } from "@/app/components/airport-analysis/TopAirportsBarChart";
import WorstAirportsBarChart from "@/app/components/airport-analysis/WorstAirportsBarChart";
import AirportRushHourHeatmap, { RushHourData } from "@/app/components/airport-analysis/AirportRushHourHeatmap";

const AirportAnalysisTab: React.FC = () => {
    const [selectedAirport, setSelectedAirport] = useState<string>("ORD");

    const bestAirports: AirportPerformance[] = [
        { airport: "ATL", onTime: 92, avgDelay: 8, totalDelayMin: 15000 },
        { airport: "DFW", onTime: 89, avgDelay: 10, totalDelayMin: 22000 },
        { airport: "DEN", onTime: 88, avgDelay: 11, totalDelayMin: 24000 },
        { airport: "CLT", onTime: 87, avgDelay: 12, totalDelayMin: 26000 },
        { airport: "LAX", onTime: 86, avgDelay: 13, totalDelayMin: 30000 },
    ];

    const worstAirports: AirportPerformance[] = [
        { airport: "EWR", onTime: 72, avgDelay: 22, totalDelayMin: 76000 },
        { airport: "JFK", onTime: 75, avgDelay: 20, totalDelayMin: 71000 },
        { airport: "SFO", onTime: 78, avgDelay: 18, totalDelayMin: 64000 },
        { airport: "ORD", onTime: 79, avgDelay: 17, totalDelayMin: 60000 },
        { airport: "BOS", onTime: 80, avgDelay: 16, totalDelayMin: 58000 },
    ];

    const rushHourData: RushHourData[] = [
        { day: "Mon", hour: 16, delayedFlights: 42 },
        { day: "Mon", hour: 17, delayedFlights: 55 },
        { day: "Fri", hour: 17, delayedFlights: 78 },
        { day: "Fri", hour: 18, delayedFlights: 92 },
        { day: "Sun", hour: 20, delayedFlights: 60 },
    ];

    return (
        <>
            {/* Filter chung cho tab */}
            <FilterBar
                enabledFilters={[
                    "dateRange",
                    "airline",
                    "originAirport",
                    "destinationAirport"
                ]}
            />

            <div className="mt-6">
                <Row gutter={24}>
                    <Col span={12}>
                        <TopAirportsBarChart
                            title="Top 5 Best Airports (Highest On-Time %)"
                            data={bestAirports}
                        />
                    </Col>

                    <Col span={12}>
                        <WorstAirportsBarChart
                            title="Top 5 Worst Airports (Highest Total Delay Minutes)"
                            data={worstAirports}
                        />
                    </Col>
                </Row>

                <div className="mt-10">
                    {/* Filter riêng cho heatmap */}
                    <FilterBar
                        enabledFilters={["airport"]}
                        onChange={(filters) => {
                            if (filters.airport.length > 0) {
                                setSelectedAirport(filters.airport[0]);
                            }
                        }}
                    />

                    <AirportRushHourHeatmap
                        airport={selectedAirport}
                        data={rushHourData}
                    />
                </div>
            </div>
        </>
    );
};

export default AirportAnalysisTab;
