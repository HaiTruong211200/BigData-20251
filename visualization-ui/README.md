# Flight Analytics Dashboard – Documentation

## Tab 1: Overview (Tổng quan)
**Mục tiêu**:
Tab này cung cấp cái nhìn tổng quan về hoạt động của các chuyến bay, giúp người dùng nhanh chóng nắm bắt được các chỉ số KPI chính và xu hướng delay/cancellation.

1. KPI chính (Hàng 1, Thẻ 1 -> 4)
- Tổng số chuyến bay: Tổng số chuyến bay được ghi nhận trong hệ thống.
- Số chuyến đúng giờ: Hiển thị cả số lượng và tỷ lệ phần trăm. Giúp đánh giá hiệu quả chung của các chuyến bay.
- Số chuyến trễ: Tổng số chuyến có thời gian trễ so với lịch bay.
- Số chuyến bị huỷ: Tổng số chuyến bị huỷ vì các nguyên nhân khác nhau.

2. KPI về độ trễ (Hàng 2)
- Tổng thời gian trễ (phút): Tổng số phút trễ tích lũy trên toàn bộ các chuyến bay.
- Thời gian trễ trung bình trên mỗi chuyến trễ: Tổng thời gian trễ chia cho số chuyến bị trễ.
- Thời gian trễ trung bình trên tất cả chuyến bay: Tổng thời gian trễ chia cho tổng số chuyến bay.

3. Biểu đồ chính

**Chart 1: Analysis of Delay Reasons (Stacked Bar Chart)**
- Mục đích: Phân tích nguyên nhân trễ chuyến theo từng loại (Weather, Carrier, System, Security, ...).
- Trục X: Thời gian (Theo filter, không filter thì mặc định past -> present)
- Trục Y: Tổng số phút trễ
- Ý nghĩa: Xác định nguyên nhân chính gây trễ, giúp điều chỉnh hoạt động hoặc cải thiện chất lượng dịch vụ.

**Chart 2: Total Flights by Cancellation Reason (Donut Chart)**
- Mục đích: Phân tích nguyên nhân huỷ chuyến.
- Các segment: Weather, Carrier, National Air System, Security, Others...
- Ý nghĩa: Giúp nhận biết nguyên nhân chủ yếu gây huỷ chuyến, từ đó đưa ra các biện pháp phòng ngừa.

**Chart 3: Flight Trends Over Time (Stacked Area Chart)**
- Mục đích: Hiển thị xu hướng số chuyến bay theo thời gian, phân biệt các trạng thái: đúng giờ, trễ, huỷ.
- Trục X: Thời gian (ngày, tuần hoặc tháng, **theo filter**..).
- Trục Y: Số lượng chuyến bay.
- Ý nghĩa: Nhìn thấy các xu hướng tổng thể, các đỉnh cao về số lượng trễ/huỷ theo mùa hoặc ngày lễ.

## Tab 2: Airline Analysis (Phân tích hãng bay)
**Mục tiêu**:
Tab này tập trung vào việc so sánh hiệu suất giữa các hãng bay, giúp nhìn thấy rõ ràng hãng nào hoạt động tốt, hãng nào gặp nhiều vấn đề về trễ hoặc huỷ chuyến.

1. **Airline Performance Leaderboard (Data Table)**

- Mục đích: Hiển thị bảng xếp hạng các hãng bay theo nhiều chỉ số: tổng chuyến bay, % đúng giờ, % trễ, % huỷ, thời gian trễ trung bình, số chuyến trễ nghiêm trọng...
- Lợi ích:
    - Dễ dàng so sánh các hãng.
    - Có thể sắp xếp dữ liệu theo từng cột để phân tích sâu hơn.

2. **Delay / Cancellation % by Airline (Bar Chart)**

- Mục đích: So sánh tỷ lệ trễ/hủy giữa các hãng.
- Trục X: Tên hãng bay
- Trục Y: Tỷ lệ phần trăm (%)
- Tính năng: Cho phép người dùng chọn xem hiển thị % trễ hay % huỷ.
- Ý nghĩa: Xác định hãng nào đáng tin cậy, hãng nào cần cải thiện dịch vụ.

3. **Delay Reason Breakdown by Airline (Stacked Bar Chart)**

- Mục đích: Hiển thị nguyên nhân trễ của từng hãng.
- Trục X: Tên hãng (Top 10 hãng)
- Trục Y: 100% (phân bổ nguyên nhân)
- Các segment: Carrier Delay, Weather Delay, Security, …
- Ý nghĩa: Giúp phân biệt trễ do lỗi hãng hay do các yếu tố bên ngoài, hỗ trợ ra quyết định và cải thiện hiệu suất.
