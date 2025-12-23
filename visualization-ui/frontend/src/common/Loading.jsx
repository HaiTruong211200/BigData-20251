import React from "react";
import { LoadingOutlined } from "@ant-design/icons";
import { Spin } from "antd";

export default function Loading() {
    return (
        <div
            style={{
                display: "flex",
                justifyContent: "center",
                alignItems: "center",
                height: "100vh",
            }}
        >
            <Spin indicator={<LoadingOutlined spin />} size="large" />
        </div>
    );
}
