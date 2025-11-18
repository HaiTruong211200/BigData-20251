'use client';

import { useState } from 'react';
import type { MenuProps } from 'antd';
import {
    Layout,
    Menu,
    theme,
    Switch,
    ConfigProvider,
    theme as antdTheme,
} from 'antd';

import OverviewTab from './tabs/OverviewTab';
import AirlineAnalysisTab from './tabs/AirlineAnalysisTab';
import AirportAnalysisTab from './tabs/AirportAnalysisTab';
import LiveMonitoringTab from './tabs/LiveMonitoringTab';

const { Header, Content } = Layout;

const items: MenuProps['items'] = [
    'Overview',
    'Airline Analysis',
    'Airport Analysis',
    'Live Monitoring & Prediction',
].map((label, index) => ({
    key: String(index + 1),
    label,
}));

export default function HomePage() {
    const [isDarkMode, setIsDarkMode] = useState(false);
    const { token: { borderRadiusLG }} = theme.useToken();
    const [activeTab, setActiveTab] = useState('1');

    const renderTab = () => {
        switch (activeTab) {
            case '1':
                return <OverviewTab />;
            case '2':
                return <AirlineAnalysisTab />
            case '3':
                return <AirportAnalysisTab />
            case '4':
                return <LiveMonitoringTab />
            default:
                return null;
        }
    }; 

    return (
        <ConfigProvider theme={{ algorithm: isDarkMode ? antdTheme.darkAlgorithm : antdTheme.defaultAlgorithm }}>
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
                        selectedKeys={[activeTab]}
                        items={items}
                        onClick={(e) => setActiveTab(e.key)}
                        style={{
                            flex: 1,
                            justifyContent: 'flex-end',
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
                    <Content
                        style={{
                            padding: 24,
                            margin: 0,
                            minHeight: 280,
                            borderRadius: borderRadiusLG,
                            overflow: 'auto',
                        }}
                    >
                        {renderTab()}
                    </Content>
                </Layout>
            </Layout>
        </ConfigProvider>
    );
}
