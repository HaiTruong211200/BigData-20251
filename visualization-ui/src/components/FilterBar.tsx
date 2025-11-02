import React, { useState } from 'react';
import { Row, Col, Select, Button, Card } from 'antd';
const { Option } = Select;

// Định nghĩa kiểu cho state filters để code chặt chẽ hơn
interface FiltersState {
    airlineCode: string[];
    airportCode: string[];
    monthName: string[];
    cancelReason: string[];
    airlineName: string[];
    airportName: string[];
}

const initialFilters: FiltersState = {
    airlineCode: [],
    airportCode: [],
    monthName: [],
    cancelReason: [],
    airlineName: [],
    airportName: [],
};

const FilterBar: React.FC = () => {
    const [filters, setFilters] = useState<FiltersState>(initialFilters);

    // Dùng keyof FiltersState để đảm bảo key là hợp lệ
    const handleChange = (key: keyof FiltersState, value: string[]) => {
        setFilters((prev) => ({ ...prev, [key]: value }));
    };

    const handleClearAll = () => {
        setFilters(initialFilters);
    };

    // Helper để tạo Select, tránh lặp code
    const renderSelect = (
        placeholder: string,
        filterKey: keyof FiltersState,
        options: { value: string; label: string }[]
    ) => (
        <Select
            mode="multiple"
            allowClear
            placeholder={placeholder}
            value={filters[filterKey]}
            onChange={(value) => handleChange(filterKey, value)}
            style={{ width: '100%' }}
        >
            {options.map((opt) => (
                <Option key={opt.value} value={opt.value}>
                    {opt.label}
                </Option>
            ))}
        </Select>
    );

    return (
        <Card variant='borderless'>
            {/* HÀNG 1: Chứa tất cả các bộ lọc 
              - xs={24}: 1 cột trên mobile
              - sm={12}: 2 cột trên tablet nhỏ
              - md={8}: 3 cột trên desktop (giống layout của bạn)
            */}
            <Row gutter={[16, 16]}>
                <Col xs={24} sm={12} md={8}>
                    {renderSelect('Airline Code', 'airlineCode', [
                        { value: 'AA', label: 'AA' },
                        { value: 'DL', label: 'DL' },
                        { value: 'UA', label: 'UA' },
                    ])}
                </Col>

                <Col xs={24} sm={12} md={8}>
                    {renderSelect('Airport Code', 'airportCode', [
                        { value: 'JFK', label: 'JFK' },
                        { value: 'LAX', label: 'LAX' },
                        { value: 'ATL', label: 'ATL' },
                    ])}
                </Col>

                <Col xs={24} sm={12} md={8}>
                    {renderSelect('Month Name', 'monthName', [
                        { value: 'January', label: 'January' },
                        { value: 'February', label: 'February' },
                        { value: 'March', label: 'March' },
                    ])}
                </Col>

                <Col xs={24} sm={12} md={8}>
                    {renderSelect('Cancel Reason', 'cancelReason', [
                        { value: 'A', label: 'A - Weather' },
                        { value: 'B', label: 'B - Maintenance' },
                        { value: 'C', label: 'C - Crew' },
                        { value: 'D', label: 'D - Security' },
                    ])}
                </Col>

                <Col xs={24} sm={12} md={8}>
                    {renderSelect('Airline Name', 'airlineName', [
                        { value: 'American Airlines', label: 'American Airlines' },
                        { value: 'Delta', label: 'Delta' },
                        { value: 'United', label: 'United' },
                    ])}
                </Col>

                <Col xs={24} sm={12} md={8}>
                    {renderSelect('Airport Name', 'airportName', [
                        { value: 'John F. Kennedy Intl', label: 'John F. Kennedy Intl' },
                        { value: 'Los Angeles Intl', label: 'Los Angeles Intl' },
                        { value: 'Hartsfield-Jackson Atlanta Intl', label: 'Hartsfield-Jackson Atlanta Intl' },
                    ])}
                </Col>
            </Row>

            {/* HÀNG 2: Chỉ chứa nút Clear All 
              - Tách riêng để dễ căn chỉnh (textAlign: 'right')
              - Xóa bỏ các style 'width' và 'height' không cần thiết trên Button
            */}
            <Row style={{ marginTop: 16 }}>
                <Col span={24} style={{ textAlign: 'right' }}>
                    <Button danger onClick={handleClearAll}>
                        Clear All
                    </Button>
                </Col>
            </Row>
        </Card>
    );
};

export default FilterBar;