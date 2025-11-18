import { Row, Col } from 'antd';
import FilterBar from '../components/FilterBar';
import SummaryCards from '../components/SummaryCards';
import DelayReasonStackedChart from '../components/DelayReasonStackedChart';
import CancellationReasonChart from '../components/CancellationReasonChart';
import FlightTrendsChart from '../components/FlightTrendsChart';

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
