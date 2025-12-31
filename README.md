
# Customer-Agent — Quick start

Mục đích: project mô phỏng gán khách hàng (`customer`) cho tư vấn viên (`agent`) theo loại gói bảo hiểm và xuất kết quả trong thư mục `data/`.

Yêu cầu môi trường
- Java 17 (Yêu cầu cho PySpark).
- Python 3.11 (khuyến nghị).
- Thư viện: xem `requirements.txt`.

Thiết lập (Windows / macOS / Linux)

1) Tạo và kích hoạt virtual environment


macOS / Linux:
```bash
python -m venv .venv
source .venv/bin/activate
```

2) Cài đặt phụ thuộc
```bash
python -m pip install -r requirements.txt
```

Chạy
- Mở JupyterLab/Notebook và chạy `src/main.ipynb` (chạy tuần tự các ô):
```bash
jupyter lab
```
- Hoặc chạy nhanh kịch bản kiểm thử (nếu có):
```bash
python src/test.py
```

Dữ liệu đầu ra
- Sau khi chạy, kết quả được lưu trong thư mục `data/`:
	- `life_health.csv` — cặp (customer_id, agent_id) cho gói life_health
	- `motor.csv` — cặp (customer_id, agent_id) cho gói motor
	- `travel.csv` — cặp (customer_id, agent_id) cho gói travel
	- `output.csv` — thông tin agent sau khi gán (đã gán bao nhiêu, còn lại bao nhiêu)

Vị trí mã nguồn chính
- Notebook chính: `src/main.ipynb`
- Sinh dữ liệu: `src/generate_data.py`
- Hàm tiện ích: `src/utils.py`

Generator — các tham số chính
- `p_ag_lh`: P(agent có khả năng tư vấn `life_health`)
- `p_ag_mt`: P(agent có khả năng tư vấn `motor`)
- `p_ag_tv`: P(agent có khả năng tư vấn `travel`)
- `p_ct_lh`: P(customer cần `life_health`)
- `p_ct_mt`: P(customer cần `motor`)
- `p_ct_tv`: P(customer cần `travel`)
- `ag_mu`: mean (mu) của phân phối chuẩn cho capacity của agent
- `ag_sm`: sigma của phân phối chuẩn cho capacity của agent
- `max_cap`: số tối đa khách 1 agent có thể nhận
- `num_customers`: số customer mô phỏng
- `num_agents`: số agent mô phỏng

Lưu ý
- Đảm bảo Java 17 có trong PATH trước khi chạy PySpark-related cells.
- Nếu cần sinh lại dữ liệu, chạy `src/generate_data.py` hoặc mở `src/main.ipynb` và chạy các ô tương ứng.

Muốn mình mở rộng README với mục ví dụ lệnh, hình minh họa hoặc hướng dẫn VS Code không?
