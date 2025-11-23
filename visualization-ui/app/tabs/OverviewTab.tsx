import { Row, Col } from 'antd';
import FilterBar from '../components/FilterBar';
import SummaryCards from '../components/overview/SummaryCards';
import DelayReasonStackedChart from '../components/overview/DelayReasonStackedChart';
import CancellationReasonChart from '../components/overview/CancellationReasonChart';
import FlightTrendsChart from '../components/overview/FlightTrendsChart';

export default function OverviewTab() {
    return (
        <>
            <FilterBar />
            <div style={{ marginTop: 24 }}>
                <SummaryCards />
            </div>
            <div style={{ marginTop: 24 }}>
                <Row gutter={[16, 16]}>
                    <Col xs={24} lg={12}>
                        <DelayReasonStackedChart />
                    </Col>
                    <Col xs={24} lg={12}>
                        <CancellationReasonChart />
                    </Col>
                </Row>
            </div>
            <div style={{ marginTop: 24 }}>
                <FlightTrendsChart />
            </div>
        </>
    );
}
