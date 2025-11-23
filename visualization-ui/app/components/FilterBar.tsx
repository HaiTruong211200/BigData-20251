"use client"

import React, { useState } from "react";
import { Row, Col, Select, DatePicker } from "antd";
const { RangePicker } = DatePicker;
const { Option } = Select;

// Định nghĩa kiểu cho state filters
interface FiltersState {
    dateRange: [string, string] | null;
    airline: string[];
    originAirport: string[];
    destinationAirport: string[];
    flightStatus: string[];
}

// State ban đầu
const initialFilters: FiltersState = {
    dateRange: null,
    airline: [],
    originAirport: [],
    destinationAirport: [],
    flightStatus: [],
};


const FilterBar: React.FC = () => {
    const [filters, setFilters] = useState<FiltersState>(initialFilters);

    const handleChange = <K extends keyof FiltersState>(
        key: K,
        value: FiltersState[K]
    ) => {
        setFilters((prev) => ({ ...prev, [key]: value }));
    };

    // Helper to render Select
    const renderSelect = (
        placeholder: string,
        filterKey: keyof FiltersState,
        options: { value: string; label: string }[]
    ) => (
        <Select
            size="large"
            mode="multiple"
            allowClear
            placeholder={placeholder}
            value={filters[filterKey] as string[]}
            onChange={(val) => handleChange(filterKey, val as any)}
            style={{ width: "100%" }}
        >
            {options.map((opt) => (
                <Option key={opt.value} value={opt.value}>
                    {opt.label}
                </Option>
            ))}
        </Select>
    );

    return (
        <Row gutter={[16, 16]}>
            {/* DATE RANGE PICKER */}
            <Col xs={24} sm={12} md={8}>
                <RangePicker
                    size="large"
                    style={{ width: "100%" }}
                    format="DD/MM/YYYY"
                    onChange={(dates, dateStrings) =>
                        handleChange("dateRange", dateStrings as [string, string])
                    }
                    placeholder={["Start Date", "End Date"]}
                />
            </Col>

            {/* AIRLINE */}
            <Col xs={24} sm={12} md={8}>
                {renderSelect("Airline (Hãng bay)", "airline", [
                    { value: "AA", label: "American Airlines" },
                    { value: "DL", label: "Delta Airlines" },
                    { value: "UA", label: "United Airlines" },
                    { value: "WN", label: "Southwest" },
                ])}
            </Col>

            {/* ORIGIN AIRPORT */}
            <Col xs={24} sm={12} md={8}>
                {renderSelect("Origin Airport (Sân bay đi)", "originAirport", [
                    { value: "JFK", label: "JFK - New York" },
                    { value: "LAX", label: "LAX - Los Angeles" },
                    { value: "ORD", label: "ORD - Chicago O'Hare" },
                    { value: "DFW", label: "DFW - Dallas/Fort Worth" },
                ])}
            </Col>

            {/* DESTINATION AIRPORT */}
            <Col xs={24} sm={12} md={8}>
                {renderSelect("Destination Airport (Sân bay đến)", "destinationAirport", [
                    { value: "ATL", label: "ATL - Atlanta" },
                    { value: "SEA", label: "SEA - Seattle" },
                    { value: "MIA", label: "MIA - Miami" },
                    { value: "SFO", label: "SFO - San Francisco" },
                ])}
            </Col>

            {/* FLIGHT STATUS
            <Col xs={24} sm={12} md={8}>
                {renderSelect("Flight Status (Trạng thái)", "flightStatus", [
                    { value: "On-Time", label: "On-Time" },
                    { value: "Delayed", label: "Delayed" },
                    { value: "Cancelled", label: "Cancelled" },
                    { value: "Diverted", label: "Diverted" },
                ])}
            </Col> */}
        </Row>
    );
};

export default FilterBar;