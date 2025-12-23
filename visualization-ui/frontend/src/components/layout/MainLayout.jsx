import { useMemo } from "react";
import { Menu } from "antd";
import { useLocation, useNavigate } from "react-router-dom";
import { Layout } from "antd";

// Chưa responsive

const { Header, Content } = Layout;

export default function MainLayout({ children }) {
    const location = useLocation();
    const navigate = useNavigate();

    const routeMap = {
        "/": "1",
        "/airline": "2",
        "/airport": "3",
        "/live": "4",
    };

    const items = useMemo(
        () => [
            { key: "1", label: "Overview", route: "/" },
            { key: "2", label: "Airline Analysis", route: "/airline" },
            { key: "3", label: "Airport Analysis", route: "/airport" },
            { key: "4", label: "Live Analysis", route: "/live" },
        ],
        []
    );

    return (
        <Layout className="min-h-screen">
            <Header className="flex justify-between items-center px-8 h-20 bg-linear-to-r from-blue-600 to-indigo-600">
                {/* Logo */}
                <div className="text-white font-bold text-xl">
                    Flight Delay Analytics
                </div>

                {/* Menu */}
                <Menu
                    theme="dark"
                    mode="horizontal"
                    selectedKeys={[routeMap[location.pathname] || "1"]}
                    onClick={(e) => {
                        const route = items.find(i => i.key === e.key)?.route;
                        if (route && route !== location.pathname) {
                            navigate(route);
                        }
                    }}
                    items={items}
                    className="bg-transparent text-white flex-1 justify-end"
                />
            </Header>

            <Content className="p-6 overflow-auto min-h-[calc(100vh-80px)]">
                {children}
            </Content>
        </Layout>
    );
}
