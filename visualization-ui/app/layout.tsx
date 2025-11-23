'use client';

import { Layout, Menu, Switch, ConfigProvider, theme as antdTheme } from 'antd';
import { usePathname, useRouter } from 'next/navigation';
import { useState } from 'react';

const { Header, Content } = Layout;

export default function RootLayout({ children }: { children: React.ReactNode }) {
    const [isDarkMode, setIsDarkMode] = useState(false);
    const pathname = usePathname();
    const router = useRouter();

    const routeMap: any = {
        '/': '1',
        '/overview': '1',
        '/airline': '2',
        '/airport': '3',
        '/live': '4',
    };

    const items = [
        { key: '1', label: 'Overview', route: '/overview' },
        { key: '2', label: 'Airline Analysis', route: '/airline' },
        { key: '3', label: 'Airport Analysis', route: '/airport' },
        { key: '4', label: 'Live Monitoring & Prediction', route: '/live' }
    ];

    return (
        <html lang="en">
            <body>
                <ConfigProvider theme={{ algorithm: isDarkMode ? antdTheme.darkAlgorithm : antdTheme.defaultAlgorithm }}>
                    <Layout style={{ height: "100vh" }}>
                        <Header style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', height: 50 }}>
                            <div style={{ color: '#fff', fontWeight: 'bold' }}>
                                Airline Flight Delays Analysis
                            </div>

                            <Menu
                                theme="dark"
                                mode="horizontal"
                                selectedKeys={[routeMap[pathname] || '1']}
                                onClick={(e) => {
                                    const route = items.find(i => i.key === e.key)?.route;
                                    router.push(route!);
                                }}
                                items={items}
                                style={{ flex: 1, justifyContent: 'flex-end' }}
                            />

                            <Switch
                                checkedChildren="🌙"
                                unCheckedChildren="☀️"
                                checked={isDarkMode}
                                onChange={setIsDarkMode}
                            />
                        </Header>

                        <Content style={{ padding: 24, overflow: 'auto' }}>
                            {children}
                        </Content>
                    </Layout>
                </ConfigProvider>
            </body>
        </html>
    );
}
