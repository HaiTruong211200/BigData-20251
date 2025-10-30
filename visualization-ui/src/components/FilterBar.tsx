import React, { useState } from 'react';
import { Row, Col, Select, Button, Card } from 'antd';
const { Option } = Select;

const FilterBar: React.FC = () => {
    const [filters, setFilters] = useState({
        airlineCode: [],
        airportCode: [],
        monthName: [],
        cancelReason: [],
        airlineName: [],
        airportName: [],
    });

    const handleChange = (key: string, value: any) => {
        setFilters((prev) => ({ ...prev, [key]: value }));
    };

    const handleClearAll = () => {
        setFilters({
            airlineCode: [],
            airportCode: [],
            monthName: [],
            cancelReason: [],
            airlineName: [],
            airportName: [],
        });
    };

    return (
        <Card
            variant='borderless'
            
        >
            {/* Hàng 1 */}
            <Row gutter={[16, 16]}>
                <Col span={8}>
                    <Select
                        className="filter-select"
                        mode="multiple"
                        allowClear
                        placeholder="Airline Code"
                        value={filters.airlineCode}
                        onChange={(value) => handleChange('airlineCode', value)}
                        style={{ width: '100%' }}
                    >
                        <Option value="AA">AA</Option>
                        <Option value="DL">DL</Option>
                        <Option value="UA">UA</Option>
                    </Select>
                </Col>

                <Col span={8}>
                    <Select
                        mode="multiple"
                        allowClear
                        placeholder="Airport Code"
                        value={filters.airportCode}
                        onChange={(value) => handleChange('airportCode', value)}
                        style={{ width: '100%' }}
                    >
                        <Option value="JFK">JFK</Option>
                        <Option value="LAX">LAX</Option>
                        <Option value="ATL">ATL</Option>
                    </Select>
                </Col>

                <Col span={8}>
                    <Select
                        className="filter-select"
                        mode="multiple"
                        allowClear
                        placeholder="Month Name"
                        value={filters.monthName}
                        onChange={(value) => handleChange('monthName', value)}
                        style={{ width: '100%' }}
                    >
                        <Option value="January">January</Option>
                        <Option value="February">February</Option>
                        <Option value="March">March</Option>
                    </Select>
                </Col>
            </Row>

            {/* Hàng 2 */}
            <Row gutter={[16, 16]} style={{ marginTop: 8 }}>
                <Col span={8}>
                    <Select
                        className="filter-select"
                        mode="multiple"
                        allowClear
                        placeholder="Cancel Reason"
                        value={filters.cancelReason}
                        onChange={(value) => handleChange('cancelReason', value)}
                        style={{ width: '100%' }}
                    >
                        <Option value="A">A - Weather</Option>
                        <Option value="B">B - Maintenance</Option>
                        <Option value="C">C - Crew</Option>
                        <Option value="D">D - Security</Option>
                    </Select>
                </Col>

                <Col span={8}>
                    <Select
                        className="filter-select"
                        mode="multiple"
                        allowClear
                        placeholder="Airline Name"
                        value={filters.airlineName}
                        onChange={(value) => handleChange('airlineName', value)}
                        style={{ width: '100%' }}
                    >
                        <Option value="American Airlines">American Airlines</Option>
                        <Option value="Delta">Delta</Option>
                        <Option value="United">United</Option>
                    </Select>
                </Col>

                <Col span={6}>
                    <Select
                        className="filter-select"
                        mode="multiple"
                        allowClear
                        placeholder="Airport Name"
                        value={filters.airportName}
                        onChange={(value) => handleChange('airportName', value)}
                        style={{ width: '100%' }}
                    >
                        <Option value="John F. Kennedy Intl">John F. Kennedy Intl</Option>
                        <Option value="Los Angeles Intl">Los Angeles Intl</Option>
                        <Option value="Hartsfield-Jackson Atlanta Intl">Hartsfield-Jackson Atlanta Intl</Option>
                    </Select>
                </Col>

                {/* Clear All */}
                <Col span={2} style={{ textAlign: 'right' }}>
                    <Button  danger onClick={handleClearAll} style={{ width: '100%', height: '110%' }}>
                        Clear All
                    </Button>
                </Col>
            </Row>
        </Card>
    );
};

export default FilterBar;
