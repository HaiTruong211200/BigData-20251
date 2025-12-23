import { BrowserRouter, Routes, Route } from "react-router-dom";
import { Suspense, lazy } from "react";

import MainLayout from "./components/layout/MainLayout";
import Loading from "./common/Loading";

const Overview = lazy(() => import("./pages/overview/Overview"));
const Airline = lazy(() => import("./pages/airline/Airline"));
const Airport = lazy(() => import("./pages/airport/Airport"));
const Live = lazy(() => import("./pages/live/Live"));

export default function App() {
    return (
        <BrowserRouter>
            <Suspense fallback={<Loading />}>
                <MainLayout>
                    <Routes>
                        <Route path="/" element={<Overview />} />
                        <Route path="/airline" element={<Airline />} />
                        <Route path="/airport" element={<Airport />} />
                        <Route path="/live" element={<Live />} />
                    </Routes>
                </MainLayout>
            </Suspense>
        </BrowserRouter>
    );
}
