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
                <Row gutter={[12, 12]}>
                  {/* Hàng 1 */}
                  <Col xs={24} sm={12} md={8} lg={6}>
                    <Card
                      variant="borderless"
                      styles={{ body: { padding: 12 } }}
                      style={{ minHeight: 'auto' }}
                    >
                      <Statistic title="Total Airlines" value={14} valueStyle={{ fontSize: 22 }} />
                    </Card>
                  </Col>
                  <Col xs={24} sm={12} md={8} lg={6}>
                    <Card
                      variant="borderless"
                      styles={{ body: { padding: 12 } }}
                      style={{ minHeight: 'auto' }}
                    >
                      <Statistic title="Total Flights" value={5.82} suffix="M" valueStyle={{ fontSize: 22 }} />
                    </Card>
                  </Col>
                  <Col xs={24} sm={12} md={8} lg={6}>
                    <Card
                      variant="borderless"
                      styles={{ body: { padding: 12 } }}
                      style={{ minHeight: 'auto' }}
                    >
                      <Statistic
                        title="On Time Flights"
                        value={3.64}
                        suffix="M"
                        valueStyle={{ color: '#3f8600', fontSize: 22 }}
                      />
                    </Card>
                  </Col>
                  <Col xs={24} sm={12} md={8} lg={6}>
                    <Card
                      variant="borderless"
                      styles={{ body: { padding: 12 } }}
                      style={{ minHeight: 'auto' }}
                    >
                      <Statistic
                        title="Delayed Flights"
                        value={2.09}
                        suffix="M"
                        valueStyle={{ color: '#cf1322', fontSize: 22 }}
                      />
                    </Card>
                  </Col>

                  {/* Hàng 2 */}
                  <Col xs={24} sm={12} md={8} lg={6}>
                    <Card
                      variant="borderless"
                      styles={{ body: { padding: 12 } }}
                      style={{ minHeight: 'auto' }}
                    >
                      <Statistic title="Cancelled Flights" value={90} suffix="K" valueStyle={{ fontSize: 22 }} />
                    </Card>
                  </Col>
                  <Col xs={24} sm={12} md={8} lg={6}>
                    <Card
                      variant="borderless"
                      styles={{ body: { padding: 12 } }}
                      style={{ minHeight: 'auto' }}
                    >
                      <Statistic
                        title="On Time %"
                        value={62.59}
                        suffix="%"
                        precision={2}
                        valueStyle={{ color: '#3f8600', fontSize: 22 }}
                      />
                    </Card>
                  </Col>
                  <Col xs={24} sm={12} md={8} lg={6}>
                    <Card
                      variant="borderless"
                      styles={{ body: { padding: 12 } }}
                      style={{ minHeight: 'auto' }}
                    >
                      <Statistic
                        title="Delayed %"
                        value={35.86}
                        suffix="%"
                        precision={2}
                        valueStyle={{ color: '#cf1322', fontSize: 22 }}
                      />
                    </Card>
                  </Col>
                  <Col xs={24} sm={12} md={8} lg={6}>
                    <Card
                      variant="borderless"
                      styles={{ body: { padding: 12 } }}
                      style={{ minHeight: 'auto' }}
                    >
                      <Statistic
                        title="Average Delay"
                        value={14.7}
                        suffix=" mins"
                        precision={1}
                        valueStyle={{ color: '#fa8c16', fontSize: 22 }}
                      />
                    </Card>
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
