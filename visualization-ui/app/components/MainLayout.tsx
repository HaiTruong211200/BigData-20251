"use client"

import { useMemo, useState } from 'react';
import { Menu, Switch, ConfigProvider, theme as antdTheme } from 'antd';
import { usePathname, useRouter } from 'next/navigation';
import { Header, Content } from 'antd/es/layout/layout';

export default function MainLayout({ children }: { children: React.ReactNode }) {
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

    const items = useMemo(() => [
        { key: '1', label: 'Overview', route: '/overview' },
        { key: '2', label: 'Airline Analysis', route: '/airline' },
        { key: '3', label: 'Airport Analysis', route: '/airport' },
        { key: '4', label: 'Live Monitoring & Prediction', route: '/live' }
    ], []);

    return (
        <ConfigProvider theme={{ algorithm: isDarkMode ? antdTheme.darkAlgorithm : antdTheme.defaultAlgorithm }}>
            <Header className="flex justify-between items-center px-8 h-20 bg-gradient-to-r from-blue-600 to-indigo-600">
                {/* Logo / Title */}
                <div className="text-white font-extrabold text-xl">
                    Flight Delay Analytics
                </div>

                {/* Menu + Switch */}
                <div className="flex items-center gap-6">
                    <Menu
                        theme="dark"
                        mode="horizontal"
                        selectedKeys={[routeMap[pathname] || '1']}
                        onClick={(e) => {
                            const route = items.find(i => i.key === e.key)?.route;
                            if (route && route !== pathname) router.push(route);
                        }}
                        items={items}
                        className="bg-transparent text-white flex-1 justify-start"
                    />

                    <Switch
                        checkedChildren="🌙"
                        unCheckedChildren="☀️"
                        checked={isDarkMode}
                        onChange={setIsDarkMode}
                    />
                </div>
            </Header>

            <Content className="p-6 overflow-auto min-h-[calc(100vh-80px)]">
                {children}
            </Content>
        </ConfigProvider>
    );
}
