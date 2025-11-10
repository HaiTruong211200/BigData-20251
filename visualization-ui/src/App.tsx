import React, { useState } from 'react';
import {
    LaptopOutlined,
    NotificationOutlined,
    UserOutlined,
    MenuUnfoldOutlined,
    MenuFoldOutlined,
} from '@ant-design/icons';
import type { MenuProps } from 'antd';
import {
    Breadcrumb,
    Layout,
    Menu,
    theme,
    Button,
    Row,
    Col,
    Card,
    Statistic,
    ConfigProvider,
    theme as antdTheme,
    Switch,
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

const App: React.FC = () => {
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
            <Layout style={{ height: '100vh' }}> {/* Fullscreen */}
                <Header
                    style={{
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'space-between',
                        height: 50,
                        padding: '0 16px',
                    }}
                >
                    <div style={{ display: 'flex', alignItems: 'center' }}>
                        {/* Nút toggle cho sider */}
                        {/* <Button
              type="text"
              icon={collapsed ? <MenuUnfoldOutlined /> : <MenuFoldOutlined />}
              onClick={toggleCollapsed}
              style={{
                fontSize: '16px',
                width: 40,
                height: 40,
                color: '#fff',
                marginRight: 8,
                marginLeft: 4,
              }}
            /> */}
                        <div style={{ color: '#fff', fontWeight: 'bold' }}>Airline Flight Delays Analysis</div>
                    </div>

                    {/* Menu ngang, Right tab */}
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
                    {/* Sidebar */}
                    {/* <Sider
            collapsible
            collapsed={collapsed}
            trigger={null}
            width={200}
            style={{
              background: colorBgContainer,
              borderRight: '1px solid #f0f0f0',
            }}
          >
            <Menu
              mode="inline"
              defaultSelectedKeys={['1']}
              defaultOpenKeys={['sub1']}
              style={{ height: '100%', borderRight: 0 }}
              items={items2}
            />
          </Sider> */}

                    <Layout>
                        {/* <Breadcrumb
              items={[{ title: 'Home' }, { title: 'List' }, { title: 'App' }]}
              style={{ margin: '16px 0' }}
            /> */}
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

                            {/* Summary Cards */}
                            <div style={{ marginTop: 24 }}>
                                <SummaryCards />
                            </div>

                            {/* Two Charts side by side */}
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
};

export default App;
