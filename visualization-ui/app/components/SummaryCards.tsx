import React from 'react';
import { Row, Col, Card, Statistic } from 'antd';

const SummaryCards: React.FC = () => {
  return (
    <Row gutter={[12, 12]}>
      <Col xs={24} sm={12} md={8} lg={4}>
        <Card variant='outlined'>
          <Statistic title="Total Airlines" value={14} valueStyle={{ fontSize: 22 }} />
        </Card>
      </Col>
      <Col xs={24} sm={12} md={8} lg={4}>
        <Card variant='outlined'>
          <Statistic title="Total Flights" value={5.82} suffix="M" valueStyle={{ fontSize: 22 }} />
        </Card>
      </Col>
      <Col xs={24} sm={12} md={8} lg={4}>
        <Card variant='outlined'>
          <Statistic
            title="On Time Flights"
            value={3.64}
            suffix="M"
            valueStyle={{ color: '#3f8600', fontSize: 22 }}
          />
        </Card>
      </Col>
      <Col xs={24} sm={12} md={8} lg={4}>
        <Card variant='outlined'>
          <Statistic
            title="Delayed Flights"
            value={2.09}
            suffix="M"
            valueStyle={{ color: '#cf1322', fontSize: 22 }}
          />
        </Card>
      </Col>

      <Col xs={24} sm={12} md={8} lg={4}>
        <Card variant='outlined'>
          <Statistic title="Cancelled Flights" value={90} suffix="K" valueStyle={{ fontSize: 22 }} />
        </Card>
      </Col>
      <Col xs={24} sm={12} md={8} lg={4}>
        <Card variant='outlined'>
          <Statistic
            title="On Time %"
            value={62.59}
            suffix="%"
            precision={2}
            valueStyle={{ color: '#3f8600', fontSize: 22 }}
          />
        </Card>
      </Col>
      {/* <Col xs={24} sm={12} md={8} lg={4}>
        <Card variant='outlined'>
          <Statistic
            title="Delayed %"
            value={35.86}
            suffix="%"
            precision={2}
            valueStyle={{ color: '#cf1322', fontSize: 22 }}
          />
        </Card>
      </Col>
      <Col xs={24} sm={12} md={8} lg={4}>
        <Card variant='outlined'>
          <Statistic
            title="Average Delay"
            value={14.7}
            suffix=" mins"
            precision={1}
            valueStyle={{ color: '#fa8c16', fontSize: 22 }}
          />
        </Card>
      </Col> */}
    </Row>
  );
};

export default SummaryCards;
