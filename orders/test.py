from production.orders.services import service_request, NOTIFICATIONS_GATEWAY, USER_DETAILS_GATEWAY, INVENTORY_GATEWAY

ORDER_GATEWAY = "http://127.0.0.1:8007"

#response = service_request(INVENTORY_GATEWAY, "inventory/timeout_reservations", {})
#print(response)

response = service_request(INVENTORY_GATEWAY, "inventory/release", {'request_id': '357c847f-9ed7-4935-b7ec-bf51b2151da4'})
#print(response)


response = service_request(ORDER_GATEWAY, "order/complete_checkout", {'company_id': 1, 'request_id': 'bcc707f6-3523-4de9-93ff-3715368c37bf', 'venue_id': 1, 'venue_order_id': 'web-basket-bkt_71450b2eac7c425b', 'reservation': {'ttl_seconds': 900, 'mode': 'all_or_nothing'}})

print(response)
