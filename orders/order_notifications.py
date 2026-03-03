import logging
from production.orders.services import service_request, NOTIFICATIONS_GATEWAY, USER_DETAILS_GATEWAY

log = logging.getLogger(__name__)


def send_notification(rid, data: dict) -> None:
    """
    Best-effort: send order notifications to all FCM tokens for company_id.
    Raises on programmer misuse (missing required args). Logs operational issues.
    """

    # programmer-contract inputs (raise if missing)
    company_id = data["company_id"]
    msg = data["msg"]

    # optional inputs
    auxilary_data = data.get("auxilary_data", {}) or {}
    msg_title = data.get("title", "New Order")
    channel_id = "order_channel"

    # fetch tokens
    fcm_resp = service_request(
        USER_DETAILS_GATEWAY,
        "notifications",
        "/user_details/fcm_token",
        {"company_id": company_id},
        rid
    )
    
    print(fcm_resp)
    
    if not fcm_resp.get("ok"):
        print(fcm_resp.get("ok"))
        log.warning("No FCM tokens for company_id=%s (skipping notification send)", company_id)
        return
        
    details = fcm_resp["data"].get("details", [])

    for item in details:
        fcm_token = item.get("fcm_token")
        if not fcm_token:
            continue

        payload = {
            "fcm_token": fcm_token,
            "title": msg_title,
            "body": msg,
            "data": auxilary_data,
            "channel_id": channel_id,
        }

        try:
            res = service_request(NOTIFICATIONS_GATEWAY, "notifications", "/notifications/send", payload, rid)
            # Optional: if your service returns {"ok": false, ...}, log it
            if isinstance(res, dict) and res.get("status") == "error":
                log.warning("Notification send failed company_id=%s token=%s res=%s", company_id, fcm_token[:12], res)
        except Exception:
            # Best-effort: keep going for other tokens
            log.exception("Notification send exception company_id=%s token=%s", company_id, fcm_token[:12])





    
    
