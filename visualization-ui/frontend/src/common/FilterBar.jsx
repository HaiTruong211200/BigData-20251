import { useState, useEffect } from "react";
import { Row, Col, Select, DatePicker } from "antd";
import { useNavigate, useLocation, useSearchParams } from "react-router-dom";
import dayjs from "dayjs";
import { getAirlinesFromDailyStats } from "../api/airline";
import { getAirportOptions } from "../api/airport";

const { RangePicker } = DatePicker;
const { Option } = Select;

// State ban đầu
const initialFilters = {
    dateRange: null,
    airline: [],
};

const MIN_DATE = dayjs("2025-01-01");
const MAX_DATE = dayjs("2025-06-30");

const disabledDate = (current) => {
    if (!current) return false;

    return (
        current.isBefore(MIN_DATE, "day") || current.isAfter(MAX_DATE, "day")
    );
};

export default function FilterBar({ enabledFilters = [], onChange }) {
    const navigate = useNavigate();
    const location = useLocation();
    const [searchParams] = useSearchParams();
    const [filters, setFilters] = useState(initialFilters);
    const [airlineOptions, setAirlineOptions] = useState([]);
    const [airportOptions, setAirportOptions] = useState([]);
    const [loadingAirports, setLoadingAirports] = useState(false);

    useEffect(() => {
        if (enabledFilters.includes("airline")) {
            getAirlinesFromDailyStats().then((data) => {
                const options = data.map((item) => ({
                    value: item.carrierCode,
                    label: `${item.carrierCode} - ${item.carrierName}`,
                }));
                setAirlineOptions(options);
            });
        }
    }, [enabledFilters]);

    useEffect(() => {
        if (enabledFilters.includes("airport")) {
            setLoadingAirports(true);

            getAirportOptions()
                .then((data) => {
                    const options = data.map((item) => ({
                        value: item.airportCode,
                        label: `${item.airportCode} - ${item.airportName}`,
                    }));

                    setAirportOptions(options);
                })
                .finally(() => {
                    setLoadingAirports(false);
                });
        }
    }, [enabledFilters]);

    const handleChange = (key, value) => {
        setFilters((prev) => {
            const newFilters = { ...prev, [key]: value };
            if (onChange) onChange(newFilters);
            return newFilters;
        });
    };

    const updateUrlParams = (key, values) => {
        const params = new URLSearchParams(searchParams.toString());

        if (values && values.length > 0) {
            params.delete(key);
            const search = `${params.toString()}${
                params.toString() ? "&" : ""
            }${key}=${values.join(",")}`;

            navigate(
                {
                    pathname: location.pathname,
                    search,
                },
                { replace: true }
            );
        } else {
            params.delete(key);
            navigate(
                {
                    pathname: location.pathname,
                    search: params.toString(),
                },
                { replace: true }
            );
        }
    };

    const renderSelect = (placeholder, filterKey, options) => (
        <Select
            size="large"
            mode="multiple"
            allowClear
            placeholder={placeholder}
            value={filters[filterKey]}
            onChange={(val) => handleChange(filterKey, val)}
            style={{ width: "100%" }}
        >
            {options.map((opt) => (
                <Option key={opt.value} value={opt.value}>
                    {opt.label}
                </Option>
            ))}
        </Select>
    );

    const airportPlaceholder = loadingAirports
        ? "Loading airports..."
        : airportOptions.length > 0
            ? `Airport (${airportOptions.length} sân bay)`
            : "Airport (Sân bay)";

    return (
        <Row gutter={[16, 16]}>
            {enabledFilters.includes("dateRange") && (
                <Col xs={24} lg={6}>
                    <RangePicker
                        size="large"
                        format="DD/MM/YYYY"
                        className="w-full"
                        defaultPickerValue={[
                            dayjs("2025-01-01"),
                            dayjs("2025-01-01"),
                        ]}
                        disabledDate={disabledDate}
                        allowClear
                        value={
                            searchParams.get("from") && searchParams.get("to")
                                ? [
                                    dayjs(searchParams.get("from")),
                                    dayjs(searchParams.get("to")),
                                ] : null
                        }
                        onChange={(dates) => {
                            const params = new URLSearchParams(
                                searchParams.toString()
                            );

                            if (!dates) {
                                params.delete("from");
                                params.delete("to");

                                navigate({
                                    pathname: location.pathname,
                                    search: params.toString(),
                                });

                                return;
                            }

                            params.set(
                                "from",
                                dayjs(dates[0]).format("YYYY-MM-DD")
                            );
                            params.set(
                                "to",
                                dayjs(dates[1]).format("YYYY-MM-DD")
                            );

                            navigate({
                                pathname: location.pathname,
                                search: params.toString(),
                            });
                        }}
                    />
                </Col>
            )}

            {enabledFilters.includes("airline") && (
                <Col xs={24} lg={6}>
                    <Select
                        size="large"
                        mode="multiple"
                        allowClear
                        placeholder="Airline (Hãng bay)"
                        style={{ width: "100%" }}
                        value={
                            searchParams.get("airline")
                                ? searchParams.get("airline").split(",")
                                : []
                        }
                        onChange={(vals) => updateUrlParams("airline", vals)}
                    >
                        {airlineOptions.map((opt) => (
                            <Option key={opt.value} value={opt.value}>
                                {opt.label}
                            </Option>
                        ))}
                    </Select>
                </Col>
            )}

            {enabledFilters.includes("originAirport") && (
                <Col xs={24} sm={12} md={8}>
                    {renderSelect(
                        "Origin Airport (Sân bay đi)",
                        "originAirport",
                        [
                            { value: "JFK", label: "JFK - New York" },
                            { value: "LAX", label: "LAX - Los Angeles" },
                            { value: "ORD", label: "ORD - Chicago O'Hare" },
                            { value: "DFW", label: "DFW - Dallas/Fort Worth" },
                        ]
                    )}
                </Col>
            )}

            {enabledFilters.includes("destinationAirport") && (
                <Col xs={24} sm={12} md={8}>
                    {renderSelect(
                        "Destination Airport (Sân bay đến)",
                        "destinationAirport",
                        [
                            { value: "ATL", label: "ATL - Atlanta" },
                            { value: "SEA", label: "SEA - Seattle" },
                            { value: "MIA", label: "MIA - Miami" },
                            { value: "SFO", label: "SFO - San Francisco" },
                        ]
                    )}
                </Col>
            )}

            {enabledFilters.includes("flightStatus") && (
                <Col xs={24} sm={12} md={8}>
                    {renderSelect(
                        "Flight Status (Trạng thái)",
                        "flightStatus",
                        [
                            { value: "On-Time", label: "On-Time" },
                            { value: "Delayed", label: "Delayed" },
                            { value: "Cancelled", label: "Cancelled" },
                            { value: "Diverted", label: "Diverted" },
                        ]
                    )}
                </Col>
            )}

            {enabledFilters.includes("airport") && (
                <Col xs={24} sm={24} md={6}>
                    <Select
                        size="large"
                        mode="multiple"
                        allowClear
                        placeholder={airportPlaceholder}
                        loading={loadingAirports}
                        notFoundContent={
                            loadingAirports ? "Loading..." : "No data"
                        }
                        style={{ width: "100%" }}
                        value={
                            searchParams.get("airport")
                                ? searchParams.get("airport").split(",")
                                : []
                        }
                        onChange={(vals) => updateUrlParams("airport", vals)}
                    >
                        {airportOptions.map((opt) => (
                            <Option key={opt.value} value={opt.value}>
                                {opt.label}
                            </Option>
                        ))}
                    </Select>
                </Col>
            )}
        </Row>
    );
}
