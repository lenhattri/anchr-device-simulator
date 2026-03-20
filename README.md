# ANCHR Device Simulator

Simulator này phát telemetry/transaction MQTT cho nhiều cột bơm và cung cấp HTTP API để scale số lượng pump đang chạy.

## Bật simulator bằng `config.json`

Mặc định binary sẽ đọc `config.json` ở thư mục làm việc hiện tại. Bạn có thể bật simulator với file cấu hình riêng bằng cờ `-config`:

```bash
go run . -config ./config.json
```

Hoặc sau khi build:

```bash
./anchr-simulator -config ./config.json
```

### Thứ tự ưu tiên cấu hình

1. Giá trị mặc định trong code.
2. File cấu hình truyền qua `-config` (`.json` hoặc `.yaml`).
3. Biến môi trường như `MQTT_HOST`, `INITIAL_PUMPS`, `HTTP_ADDR`.

Điều này có nghĩa là bạn có thể bật phần lớn cấu hình bằng `config.json`, rồi chỉ override vài giá trị bằng env nếu cần.

### Ghi chú về `simulation.speed`

`speed` là hệ số time-scale của simulator:

- Tăng `speed` sẽ rút ngắn thời gian chờ thực tế giữa các lần publish.
- Simulator vẫn giữ mật độ telemetry theo thời gian mô phỏng (không còn bị giảm số tick khi tăng speed).
- Ví dụ `speed: 10` nghĩa là chu kỳ chờ 1 giây mô phỏng chỉ còn ~100ms ngoài đời thực.

### Ghi chú về `transaction.publish_timeout`

- `transaction.publish_timeout` là thời gian tối đa chờ PUBACK cho mỗi lần publish transaction.
- Giá trị quá lớn sẽ làm tx bị treo lâu sau khi vừa kết thúc phiên bơm nếu broker phản hồi chậm hoặc mất ACK.
- Mặc định hiện tại là `10` giây để tránh retry quá sớm khi broker đang bận, nhưng bạn vẫn có thể giảm giá trị này nếu muốn tx retry nhanh hơn.

### Debug tx timing

- Bật `DEBUG_TX_TIMING=true` để log chi tiết từng giai đoạn tx trong simulator.
- Log sẽ cho biết tx đang chậm ở bước `reserved`, `published`, hay `confirmed`, cùng các mốc `stage_latency_ms`, `since_end_ms`, `reserve_ms`, `publish_ms`, `confirm_ms`.
- Mục đích là để phân biệt tx chậm do simulator giữ lại trước khi publish hay do downstream ingest/Kafka.

## Chạy bằng Docker Compose

`docker-compose.yml` đã được cấu hình để mount file `config.json` trong repo vào container và khởi động simulator với file này:

```bash
docker compose up --build
```

Nếu muốn bật/tắt hoặc chỉnh simulator, chỉ cần sửa `config.json` rồi chạy lại compose. File này là nguồn cấu hình chính cho cả local run và Docker Compose.

### Các service

- `emqx`: MQTT broker để simulator publish/subscribe.
- `simulator`: binary Go, đọc `/app/config.json` trong container.

### Dữ liệu persist

Compose vẫn mount volume `simulator-state` vào `/data`. Nếu muốn state file nằm trong volume này, hãy đổi trong `config.json`:

```json
{
  "persistence": {
    "tx_state_file": "/data/sim-tx-state.json"
  }
}
```

## API scale

Sau khi bật compose/local run, API HTTP mặc định lắng nghe ở `:8080`.

- Xem trạng thái:

```bash
curl http://localhost:8080/status
```

- Scale số pump:

```bash
curl -X POST http://localhost:8080/scale \
  -H 'Content-Type: application/json' \
  -d '{"target_pumps": 2000}'
```

## File cấu hình mẫu

Repo giữ sẵn cả:

- `config.json`: cấu hình mặc định cho runtime hiện tại.
- `config.yaml`: tương thích ngược nếu bạn vẫn muốn chạy bằng YAML.
