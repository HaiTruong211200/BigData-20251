'use client'; // bắt buộc nếu dùng useState, useEffect trong Next 13 App Router

import React, { useState } from 'react';
import {
    LaptopOutlined,
    NotificationOutlined,
    UserOutlined,
} from '@ant-design/icons';
import type { MenuProps } from 'antd';
import {
    Breadcrumb,
    Layout,
    Menu,
    theme,
    Switch,
    Row,
    Col,
    ConfigProvider,
    theme as antdTheme,
} from 'antd';
import FilterBar from './components/FilterBar';
import SummaryCards from './components/SummaryCards';
import FlightAnalysisChart from './components/FlightAnalysisChart';
import CancellationReasonChart from './components/CancellationReasonChart';

const { Header, Content, Sider } = Layout;

const items1: MenuProps['items'] = [
    'Summary',
    'Airline Analysis',
    'Time Analysis',
    'Airport Analysis',
    'Delay Analysis',
].map((label, index) => ({
    key: String(index + 1),
    label,
}));

const items2: MenuProps['items'] = [UserOutlined, LaptopOutlined, NotificationOutlined].map(
    (icon, index) => {
        const key = String(index + 1);
        return {
            key: `sub${key}`,
            icon: React.createElement(icon),
            label: `subnav ${key}`,
            children: Array.from({ length: 4 }).map((_, j) => {
                const subKey = index * 4 + j + 1;
                return {
                    key: subKey,
                    label: `option${subKey}`,
                };
            }),
        };
    }
);

export default function HomePage() {
    const [collapsed, setCollapsed] = useState(false);
    const [isDarkMode, setIsDarkMode] = useState(false);

    const {
        token: { colorBgContainer, borderRadiusLG },
    } = theme.useToken();

    const toggleCollapsed = () => {
        setCollapsed(!collapsed);
    };

    return (
        <ConfigProvider
            theme={{
                algorithm: isDarkMode ? antdTheme.darkAlgorithm : antdTheme.defaultAlgorithm,
            }}
        >
            <Layout style={{ height: '100vh' }}>
                <Header
                    style={{
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'space-between',
                        height: 50,
                        padding: '0 16px',
                    }}
                >
                    <div style={{ color: '#fff', fontWeight: 'bold' }}>
                        Airline Flight Delays Analysis
                    </div>
                    <Menu
                        theme="dark"
                        mode="horizontal"
                        defaultSelectedKeys={['1']}
                        items={items1}
                        style={{
                            flex: 1,
                            justifyContent: 'flex-end',
                            alignItems: 'center',
                            lineHeight: '50px',
                            height: 50,
                        }}
                    />
                    <Switch
                        checkedChildren="🌙"
                        unCheckedChildren="☀️"
                        checked={isDarkMode}
                        onChange={setIsDarkMode}
                    />
                </Header>

                <Layout>
                    <Layout>
                        <Content
                            style={{
                                padding: 24,
                                margin: 0,
                                minHeight: 280,
                                borderRadius: borderRadiusLG,
                                overflow: 'auto',
                            }}
                        >
                            <FilterBar />
                            <div style={{ marginTop: 24 }}>
                                <SummaryCards />
                            </div>
                            <div style={{ marginTop: 24 }}>
                                <Row gutter={[16, 16]}>
                                    <Col xs={24} lg={12}>
                                        <FlightAnalysisChart />
                                    </Col>
                                    <Col xs={24} lg={12}>
                                        <CancellationReasonChart />
                                    </Col>
                                </Row>
                            </div>
                        </Content>
                    </Layout>
                </Layout>
            </Layout>
        </ConfigProvider>
    );
}
