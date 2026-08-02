#!/usr/bin/env bash
# RabbitMQ vhost 初始化(可选,需要新 vhost 时才跑)
# 创建 vhost `fastkit` 并给默认用户 `guest` 授权。
# 在 RabbitMQ 容器内执行,例如:
#   docker exec <rabbitmq-container> /scripts/rabbitmq-init.sh
set -euo pipefail

rabbitmqctl add_vhost fastkit \
  && rabbitmqctl set_permissions -p fastkit guest ".*" ".*" ".*"

echo
echo "Done. broker_url = amqp://guest:***@<host>:5672/fastkit"

# 删除 fastkit_test vhost 所有队列(手动兜底;集成测试 session 开跑前已自动清理,
# 见 tests/integration/conftest.py::clean_test_vhost):
# rabbitmqctl list_queues -p fastkit_test name | awk '/^name$/{found=1; next} found' | xargs -I{} rabbitmqctl delete_queue -p fastkit_test {}
