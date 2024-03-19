bin/lakehouse-test consume \
-t 2 \
-u pulsar://localhost:6650 \
-w http://localhost:8080 \
-ss test-sub \
lakehouse_delta_test_v1 > logs/lakehouse-test.log 2>&1 &