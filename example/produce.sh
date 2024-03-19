bin/lakehouse-test produce \
-t 10 \
-r 5000 \
-u pulsar://localhost:6650 \
lakehouse_delta_test_v1 > logs/lakehouse-test.log 2>&1 &