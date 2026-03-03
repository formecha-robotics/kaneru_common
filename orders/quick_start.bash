#!/bin/bash
cd ~/yahoo_extraction/tokyo_bookshelf/camera_scan
export JWT_SIGNING_KID="order_gateway"
export JWT_PUBLIC_KEY_PATH="./secrets/jwt/public_for_orders.pem"
export JWT_PRIVATE_KEY_PATH="./secrets/jwt/order/private.pem"
export SERVICE_NAME="order_gateway"
export KANERU_ORDERS_REDIS_ADDR_KEY_B64="wXcK2ZxTtF9pR7G5bqvYHc2n8uQmL1r0sJkP4tV9dA8="

python3 ./production/orders/order_gateway.py

