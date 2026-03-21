from typing import Dict, List, Tuple, Any, Optional
from collections import defaultdict
import requests

from typing import Dict, Tuple
from collections import defaultdict

def cancel_committed_order(
    cur,
    venue_id: int,
    venue_order_id: str,
) -> int:
    """
    Cancels a fully committed (but not shipped) order.

    Effects (atomic, single transaction):
      - inv_location_stock: allocated -= qty_committed
      - inventory:          allocated -= qty_committed
      - inv_order_commit.status: COMMITTED -> CANCELLED

    Notes:
      - Does NOT change quantity (quantity is only reduced at SHIPPED).
      - Idempotent: if already CANCELLED, no-op.
      - Rejects if status is SHIPPED (returns/refunds are a different flow).

    Returns:
      inv_order_commit_id

    Raises:
      RuntimeError on missing commit, bad status, missing rows, or guard failures.
    """

    # 1) Lock commit header for this order
    cur.execute(
        """
        SELECT inv_order_commit_id, status
        FROM inv_order_commit
        WHERE venue_id = %s AND venue_order_id = %s
        FOR UPDATE
        """,
        (venue_id, venue_order_id),
    )
    header = cur.fetchone()
    if not header:
        raise RuntimeError(f"No inv_order_commit found for venue_id={venue_id}, venue_order_id={venue_order_id}")

    commit_id = int(header["inv_order_commit_id"])
    status = header["status"]

    # Idempotency
    if status == "CANCELLED":
        return commit_id

    if status == "SHIPPED":
        raise RuntimeError(
            f"Cannot cancel: order already SHIPPED for venue_id={venue_id}, venue_order_id={venue_order_id}"
        )

    if status != "COMMITTED":
        raise RuntimeError(
            f"Cannot cancel order in status={status} for venue_id={venue_id}, venue_order_id={venue_order_id}"
        )

    # 2) Lock commit lines and aggregate quantities
    cur.execute(
        """
        SELECT inv_id, inv_location_id, qty_committed
        FROM inv_order_commit_lines
        WHERE inv_order_commit_id = %s
        FOR UPDATE
        """,
        (commit_id,),
    )
    lines = cur.fetchall()
    if not lines:
        raise RuntimeError(f"No inv_order_commit_lines found for inv_order_commit_id={commit_id}")

    by_inv_loc: Dict[Tuple[int, int], float] = defaultdict(float)
    by_inv: Dict[int, float] = defaultdict(float)

    for r in lines:
        inv_id = int(r["inv_id"])
        inv_location_id = int(r["inv_location_id"])
        qty = float(r["qty_committed"])
        if qty <= 0:
            continue
        by_inv_loc[(inv_id, inv_location_id)] += qty
        by_inv[inv_id] += qty

    if not by_inv_loc:
        raise RuntimeError(f"Commit lines contain no positive quantities for inv_order_commit_id={commit_id}")

    # 3) Lock affected inventory rows FOR UPDATE
    inv_ids = sorted(by_inv.keys())
    inv_placeholders = ",".join(["%s"] * len(inv_ids))
    cur.execute(
        f"""
        SELECT inv_id
        FROM inventory
        WHERE inv_id IN ({inv_placeholders})
        FOR UPDATE
        """,
        inv_ids,
    )
    locked_inv = {int(x["inv_id"]) for x in cur.fetchall()}
    missing_inv = [i for i in inv_ids if i not in locked_inv]
    if missing_inv:
        raise RuntimeError(f"Missing inventory rows for inv_id(s): {missing_inv}")

    # 4) Lock affected inv_location_stock rows FOR UPDATE (exact pairs)
    pair_clauses = []
    pair_args = []
    for (inv_id, inv_location_id) in sorted(by_inv_loc.keys()):
        pair_clauses.append("(inv_id=%s AND inv_location_id=%s)")
        pair_args.extend([inv_id, inv_location_id])

    cur.execute(
        f"""
        SELECT inv_id, inv_location_id
        FROM inv_location_stock
        WHERE {" OR ".join(pair_clauses)}
        FOR UPDATE
        """,
        tuple(pair_args),
    )
    locked_pairs = {(int(x["inv_id"]), int(x["inv_location_id"])) for x in cur.fetchall()}
    missing_pairs = [p for p in by_inv_loc.keys() if p not in locked_pairs]
    if missing_pairs:
        raise RuntimeError(f"Missing inv_location_stock rows for pairs: {missing_pairs}")

    # 5) Apply location-level unallocate: allocated--
    for (inv_id, inv_location_id), qty in sorted(by_inv_loc.items()):
        cur.execute(
            """
            UPDATE inv_location_stock
            SET allocated = allocated - %s
            WHERE inv_id = %s AND inv_location_id = %s
              AND allocated >= %s
            """,
            (qty, inv_id, inv_location_id, qty),
        )
        if cur.rowcount != 1:
            raise RuntimeError(
                f"inv_location_stock cancel failed for inv_id={inv_id}, inv_location_id={inv_location_id}, qty={qty} "
                f"(rowcount={cur.rowcount})"
            )

    # 6) Apply aggregate-level unallocate: allocated--
    for inv_id, qty in sorted(by_inv.items()):
        cur.execute(
            """
            UPDATE inventory
            SET allocated = allocated - %s
            WHERE inv_id = %s
              AND allocated >= %s
            """,
            (qty, inv_id, qty),
        )
        if cur.rowcount != 1:
            raise RuntimeError(
                f"inventory cancel failed for inv_id={inv_id}, qty={qty} (rowcount={cur.rowcount})"
            )

    # 7) Mark commit as CANCELLED
    cur.execute(
        """
        UPDATE inv_order_commit
        SET status = 'CANCELLED'
        WHERE inv_order_commit_id = %s AND status = 'COMMITTED'
        """,
        (commit_id,),
    )
    if cur.rowcount != 1:
        raise RuntimeError(f"Failed to update inv_order_commit to CANCELLED for inv_order_commit_id={commit_id}")

    return commit_id


def ship_committed_order(
    cur,
    venue_id: int,
    venue_order_id: str,
) -> int:
    """
    Atomically marks an order as SHIPPED by consuming the *entire* committed allocation:

      inv_location_stock: allocated -= qty, quantity -= qty
      inventory:          allocated -= qty, quantity -= qty
      inv_order_commit.status: COMMITTED -> SHIPPED

    Concurrency-safe (InnoDB):
      - Locks the commit header row FOR UPDATE
      - Locks commit lines FOR UPDATE
      - Locks affected inventory + inv_location_stock rows FOR UPDATE
      - Applies guarded updates to prevent negatives

    Idempotent:
      - If already SHIPPED, no-op and returns inv_order_commit_id.

    Returns:
      inv_order_commit_id

    Raises:
      RuntimeError if commit missing, wrong status, missing rows, or update guards fail.
    """

    # 1) Lock commit header for this order
    cur.execute(
        """
        SELECT inv_order_commit_id, status
        FROM inv_order_commit
        WHERE venue_id = %s AND venue_order_id = %s
        FOR UPDATE
        """,
        (venue_id, venue_order_id),
    )
    header = cur.fetchone()
    if not header:
        raise RuntimeError(f"No inv_order_commit found for venue_id={venue_id}, venue_order_id={venue_order_id}")

    commit_id = int(header["inv_order_commit_id"])
    status = header["status"]

    # Idempotency: already shipped => no-op
    if status == "SHIPPED":
        return commit_id

    if status != "COMMITTED":
        raise RuntimeError(
            f"Cannot ship order in status={status} for venue_id={venue_id}, venue_order_id={venue_order_id}"
        )

    # 2) Lock commit lines and aggregate quantities per (inv_id, inv_location_id) and per inv_id
    cur.execute(
        """
        SELECT inv_id, inv_location_id, qty_committed
        FROM inv_order_commit_lines
        WHERE inv_order_commit_id = %s
        FOR UPDATE
        """,
        (commit_id,),
    )
    lines = cur.fetchall()
    if not lines:
        raise RuntimeError(f"No inv_order_commit_lines found for inv_order_commit_id={commit_id}")

    by_inv_loc: Dict[Tuple[int, int], float] = defaultdict(float)
    by_inv: Dict[int, float] = defaultdict(float)

    for r in lines:
        inv_id = int(r["inv_id"])
        inv_location_id = int(r["inv_location_id"])
        qty = float(r["qty_committed"])
        if qty <= 0:
            continue
        by_inv_loc[(inv_id, inv_location_id)] += qty
        by_inv[inv_id] += qty

    if not by_inv_loc:
        raise RuntimeError(f"Commit lines contain no positive quantities for inv_order_commit_id={commit_id}")

    # 3) Lock the affected inventory rows FOR UPDATE
    inv_ids = sorted(by_inv.keys())
    inv_placeholders = ",".join(["%s"] * len(inv_ids))
    cur.execute(
        f"""
        SELECT inv_id
        FROM inventory
        WHERE inv_id IN ({inv_placeholders})
        FOR UPDATE
        """,
        inv_ids,
    )
    locked_inv = {int(x["inv_id"]) for x in cur.fetchall()}
    missing_inv = [i for i in inv_ids if i not in locked_inv]
    if missing_inv:
        raise RuntimeError(f"Missing inventory rows for inv_id(s): {missing_inv}")

    # 4) Lock the affected inv_location_stock rows FOR UPDATE (exact pairs)
    pair_clauses = []
    pair_args = []
    for (inv_id, inv_location_id) in sorted(by_inv_loc.keys()):
        pair_clauses.append("(inv_id=%s AND inv_location_id=%s)")
        pair_args.extend([inv_id, inv_location_id])

    cur.execute(
        f"""
        SELECT inv_id, inv_location_id
        FROM inv_location_stock
        WHERE {" OR ".join(pair_clauses)}
        FOR UPDATE
        """,
        tuple(pair_args),
    )
    locked_pairs = {(int(x["inv_id"]), int(x["inv_location_id"])) for x in cur.fetchall()}
    missing_pairs = [p for p in by_inv_loc.keys() if p not in locked_pairs]
    if missing_pairs:
        raise RuntimeError(f"Missing inv_location_stock rows for pairs: {missing_pairs}")

    # 5) Apply location-level consumption: allocated-- and quantity--
    for (inv_id, inv_location_id), qty in sorted(by_inv_loc.items()):
        cur.execute(
            """
            UPDATE inv_location_stock
            SET
              allocated = allocated - %s,
              quantity  = quantity  - %s
            WHERE inv_id = %s AND inv_location_id = %s
              AND allocated >= %s
              AND quantity  >= %s
            """,
            (qty, qty, inv_id, inv_location_id, qty, qty),
        )
        if cur.rowcount != 1:
            raise RuntimeError(
                f"inv_location_stock ship failed for inv_id={inv_id}, inv_location_id={inv_location_id}, qty={qty} "
                f"(rowcount={cur.rowcount})"
            )

    # 6) Apply aggregate-level consumption: allocated-- and quantity--
    for inv_id, qty in sorted(by_inv.items()):
        cur.execute(
            """
            UPDATE inventory
            SET
              allocated = allocated - %s,
              quantity  = quantity  - %s
            WHERE inv_id = %s
              AND allocated >= %s
              AND quantity  >= %s
            """,
            (qty, qty, inv_id, qty, qty),
        )
        if cur.rowcount != 1:
            raise RuntimeError(
                f"inventory ship failed for inv_id={inv_id}, qty={qty} (rowcount={cur.rowcount})"
            )

    # 7) Mark commit as SHIPPED (still locked)
    cur.execute(
        """
        UPDATE inv_order_commit
        SET status = 'SHIPPED'
        WHERE inv_order_commit_id = %s AND status = 'COMMITTED'
        """,
        (commit_id,),
    )
    if cur.rowcount != 1:
        raise RuntimeError(f"Failed to update inv_order_commit to SHIPPED for inv_order_commit_id={commit_id}")

    return commit_id


def commit_reservation_to_allocated(
    cur,
    venue_id: int,
    venue_order_id: str,
    reservation_id: str,  # this is request_id in inv_order_reservations
) -> int:
    """
    Atomically converts a reservation (reserved) into an allocation (allocated),
    and writes inv_order_commit + inv_order_commit_lines.

    Transactional + concurrency-safe (InnoDB):
      - Locks all reservation rows for reservation_id (FOR UPDATE)
      - Rejects if any row is expired
      - Locks corresponding inventory + inv_location_stock rows (FOR UPDATE)
      - Moves reserved -> allocated (both tables)
      - Inserts commit header + lines
      - Deletes reservation rows (so they can't later be released)

    Returns:
      inv_order_commit_id

    Raises:
      RuntimeError on expiry, missing rows, or unexpected rowcounts.
    """

    # --- 0) Idempotency: if already committed for this order, no-op ---
    cur.execute(
        """
        SELECT inv_order_commit_id
        FROM inv_order_commit
        WHERE venue_id = %s AND venue_order_id = %s
        LIMIT 1
        """,
        (venue_id, venue_order_id),
    )
    row = cur.fetchone()
    if row:
        return int(row["inv_order_commit_id"])

    # --- 1) Lock reservation rows + check expiry ---
    cur.execute(
        """
        SELECT
          inv_id,
          inv_location_id,
          reserved,
          reservation_time,
          ttl_seconds,
          (reservation_time + INTERVAL ttl_seconds SECOND) AS expires_at,
          NOW() AS now_time
        FROM inv_order_reservations
        WHERE request_id = %s
        FOR UPDATE
        """,
        (reservation_id,),
    )
    res_rows = cur.fetchall()
    if not res_rows:
        raise RuntimeError(f"No reservation rows found for reservation_id={reservation_id}")

    # expiry: fail if any line is expired
    for r in res_rows:
        expires_at = r["expires_at"]
        now_time = r["now_time"]
        if expires_at is None:
            raise RuntimeError(f"Reservation row missing expires_at for reservation_id={reservation_id}")
        if now_time >= expires_at:
            raise RuntimeError(
                f"Reservation expired for reservation_id={reservation_id} (now={now_time}, expires_at={expires_at})"
            )

    # Aggregate quantities per (inv_id, inv_location_id) and per inv_id
    by_inv_loc: Dict[Tuple[int, int], float] = defaultdict(float)
    by_inv: Dict[int, float] = defaultdict(float)

    for r in res_rows:
        inv_id = int(r["inv_id"])
        inv_location_id = int(r["inv_location_id"])
        qty = float(r["reserved"])
        if qty <= 0:
            continue
        by_inv_loc[(inv_id, inv_location_id)] += qty
        by_inv[inv_id] += qty

    if not by_inv_loc:
        raise RuntimeError(f"Reservation contains no positive quantities for reservation_id={reservation_id}")

    # --- 2) Lock the stock rows we're about to update (prevents weird interleavings) ---
    inv_ids = sorted(by_inv.keys())
    inv_placeholders = ",".join(["%s"] * len(inv_ids))

    cur.execute(
        f"""
        SELECT inv_id
        FROM inventory
        WHERE inv_id IN ({inv_placeholders})
        FOR UPDATE
        """,
        inv_ids,
    )
    locked_inv = cur.fetchall()
    if len(locked_inv) != len(inv_ids):
        found = {int(x["inv_id"]) for x in locked_inv}
        missing = [i for i in inv_ids if i not in found]
        raise RuntimeError(f"Missing inventory rows for inv_id(s): {missing}")

    # Lock inv_location_stock rows for the exact (inv_id, inv_location_id) pairs
    # Build OR clause for exact pairs (keeps it exact and indexed if you have (inv_id, inv_location_id) key)
    pair_clauses = []
    pair_args = []
    for (inv_id, inv_location_id) in sorted(by_inv_loc.keys()):
        pair_clauses.append("(inv_id=%s AND inv_location_id=%s)")
        pair_args.extend([inv_id, inv_location_id])

    cur.execute(
        f"""
        SELECT inv_id, inv_location_id
        FROM inv_location_stock
        WHERE {" OR ".join(pair_clauses)}
        FOR UPDATE
        """,
        tuple(pair_args),
    )
    locked_pairs = {(int(x["inv_id"]), int(x["inv_location_id"])) for x in cur.fetchall()}
    missing_pairs = [p for p in by_inv_loc.keys() if p not in locked_pairs]
    if missing_pairs:
        raise RuntimeError(f"Missing inv_location_stock rows for pairs: {missing_pairs}")

    # --- 3) Apply reserved -> allocated moves (guard against negatives) ---
    for (inv_id, inv_location_id), qty in sorted(by_inv_loc.items()):
        cur.execute(
            """
            UPDATE inv_location_stock
            SET
              reserved  = reserved  - %s,
              allocated = allocated + %s
            WHERE inv_id = %s AND inv_location_id = %s
              AND reserved >= %s
            """,
            (qty, qty, inv_id, inv_location_id, qty),
        )
        if cur.rowcount != 1:
            raise RuntimeError(
                f"inv_location_stock move failed for inv_id={inv_id}, inv_location_id={inv_location_id}, qty={qty} (rowcount={cur.rowcount})"
            )

    for inv_id, qty in sorted(by_inv.items()):
        cur.execute(
            """
            UPDATE inventory
            SET
              reserved  = reserved  - %s,
              allocated = allocated + %s
            WHERE inv_id = %s
              AND reserved >= %s
            """,
            (qty, qty, inv_id, qty),
        )
        if cur.rowcount != 1:
            raise RuntimeError(
                f"inventory move failed for inv_id={inv_id}, qty={qty} (rowcount={cur.rowcount})"
            )

    # --- 4) Insert commit header (idempotent via UNIQUE(venue_id, venue_order_id)) ---
    # Use LAST_INSERT_ID trick so cur.lastrowid works for both insert and duplicate.
    cur.execute(
        """
        INSERT INTO inv_order_commit
          (venue_id, venue_order_id, request_id, status)
        VALUES
          (%s, %s, %s, 'COMMITTED')
        ON DUPLICATE KEY UPDATE
          inv_order_commit_id = LAST_INSERT_ID(inv_order_commit_id)
        """,
        (venue_id, venue_order_id, reservation_id),
    )
    commit_id = int(cur.lastrowid)
    if commit_id <= 0:
        raise RuntimeError("Failed to obtain inv_order_commit_id")

    # --- 5) Insert commit lines ---
    for (inv_id, inv_location_id), qty in sorted(by_inv_loc.items()):
        cur.execute(
            """
            INSERT INTO inv_order_commit_lines
              (inv_order_commit_id, inv_id, inv_location_id, qty_committed)
            VALUES
              (%s, %s, %s, %s)
            """,
            (commit_id, inv_id, inv_location_id, qty),
        )
        if cur.rowcount != 1:
            raise RuntimeError(
                f"inv_order_commit_lines insert failed commit_id={commit_id}, inv_id={inv_id}, inv_location_id={inv_location_id} (rowcount={cur.rowcount})"
            )

    # --- 6) Consume the reservation rows (prevents later release) ---
    cur.execute(
        """
        DELETE FROM inv_order_reservations
        WHERE request_id = %s
        """,
        (reservation_id,),
    )
    if cur.rowcount < 1:
        raise RuntimeError(f"Expected to delete reservation rows for reservation_id={reservation_id}, deleted={cur.rowcount}")

    return commit_id



def fetch_expired_request_ids(cur, limit: int) -> List[str]:
    cur.execute(
        """
        SELECT request_id
        FROM inv_order_reservations
        WHERE expires_at < NOW()
        GROUP BY request_id
        ORDER BY MIN(expires_at) ASC
        LIMIT %s
        """,
        (limit,),
    )
    return [r["request_id"] for r in cur.fetchall()]


def release_reservations(cur, request_id: str) -> int:
    """
    Release (undo) all reservations for a given request_id.

    Steps (single transaction, caller controls commit/rollback):
      1) SELECT reservation rows FOR UPDATE (locks them)
      2) Aggregate quantities per (inv_id, inv_location_id) and per inv_id
      3) UPDATE inv_location_stock.reserved -= qty  for each (inv_id, inv_location_id)
      4) UPDATE inventory.reserved          -= qty  for each inv_id
      5) DELETE reservation rows for request_id
    Returns:
      Number of inv_order_reservations rows deleted.
    Raises:
      RuntimeError on unexpected rowcount / missing rows.
    """

    # 1) Lock reservation rows for this request
    cur.execute(
        """
        SELECT inv_id, inv_location_id, reserved
        FROM inv_order_reservations
        WHERE request_id = %s
        FOR UPDATE
        """,
        (request_id,),
    )
    rows = cur.fetchall()
    if not rows:
        return 0  # nothing to release

    # 2) Aggregate
    by_inv_loc: Dict[Tuple[int, int], float] = defaultdict(float)
    by_inv: Dict[int, float] = defaultdict(float)

    for r in rows:
        inv_id = int(r["inv_id"])
        inv_location_id = int(r["inv_location_id"])
        qty = float(r["reserved"])
        if qty <= 0:
            continue
        by_inv_loc[(inv_id, inv_location_id)] += qty
        by_inv[inv_id] += qty

    # 3) Decrement location-level reserved (with guard to avoid negative)
    for (inv_id, inv_location_id), qty in by_inv_loc.items():
        cur.execute(
            """
            UPDATE inv_location_stock
            SET reserved = reserved - %s
            WHERE inv_id = %s AND inv_location_id = %s
              AND reserved >= %s
            """,
            (qty, inv_id, inv_location_id, qty),
        )
        if cur.rowcount != 1:
            raise RuntimeError(
                f"release failed: inv_location_stock inv_id={inv_id}, inv_location_id={inv_location_id}, qty={qty} (rowcount={cur.rowcount})"
            )

    # 4) Decrement aggregate reserved (with guard)
    for inv_id, qty in by_inv.items():
        cur.execute(
            """
            UPDATE inventory
            SET reserved = reserved - %s
            WHERE inv_id = %s
              AND reserved >= %s
            """,
            (qty, inv_id, qty),
        )
        if cur.rowcount != 1:
            raise RuntimeError(
                f"release failed: inventory inv_id={inv_id}, qty={qty} (rowcount={cur.rowcount})"
            )

    # 5) Delete reservation rows (still locked by FOR UPDATE)
    cur.execute(
        """
        DELETE FROM inv_order_reservations
        WHERE request_id = %s
        """,
        (request_id,),
    )
    deleted = cur.rowcount

    # Sanity: we expect to delete at least what we selected.
    # (If you have triggers or other logic, rowcount could differ, but usually it should match.)
    if deleted < len(rows):
        raise RuntimeError(
            f"release failed: deleted fewer rows than selected (selected={len(rows)}, deleted={deleted})"
        )

    return deleted

def reserve_inventory_rows(
    cur,
    request_id,
    venue_id,
    venue_order_id,
    ttl_seconds,
    inv_map: Dict[int, List[Dict[str, Any]]],
) -> None:
        
    for inv_id, inv_data in inv_map.items():
        inv_id = int(inv_id)
        rows = inv_data['reservations']
        company_id = inv_data['company_id']

        for loc_qty in rows:
            inv_location_id = int(loc_qty["inv_location_id"])
            qty = float(loc_qty["reserve"])
            if qty <= 0:
                continue

            cur.execute(
                """
                UPDATE inv_location_stock
                SET reserved = reserved + %s
                WHERE inv_id = %s AND inv_location_id = %s
                """,
                (qty, inv_id, inv_location_id),
            )
            if cur.rowcount != 1:
                raise RuntimeError(
                    f"inv_location_stock update failed for inv_id={inv_id}, inv_location_id={inv_location_id} (rowcount={cur.rowcount})"
                )

            cur.execute(
                """
                INSERT INTO inv_order_reservations
                (request_id, venue_id, venue_order_id, ttl_seconds, inv_id, inv_location_id, reserved, company_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (request_id, venue_id, venue_order_id, ttl_seconds, inv_id, inv_location_id, qty, company_id),
            )
            if cur.rowcount != 1:
                raise RuntimeError(
                    f"inv_order_reservations insert failed for request_id={request_id}, inv_id={inv_id}, inv_location_id={inv_location_id} (rowcount={cur.rowcount})"
                )
                
            cur.execute(
                """
                UPDATE inventory
                SET reserved = reserved + %s
                WHERE inv_id = %s
                """,
                (qty, inv_id),
            )
            if cur.rowcount != 1:
                raise RuntimeError(
                    f"inventory update failed for inv_id={inv_id} (rowcount={cur.rowcount})"
                )

    cur.execute(
        """
        SELECT * FROM inv_order_reservations
        WHERE request_id = %s
        """,
        (request_id,),
    )
    response_data = cur.fetchall()

    return response_data

def check_availability(cur, inv_lines: List[dict]) -> Tuple[bool, List[dict], Dict[int, List[dict]]]:
    inv_ids = [int(x["inv_id"]) for x in inv_lines]
    if not inv_ids:
        return True, [], {}

    placeholders = ",".join(["%s"] * len(inv_ids))
    sql = f"""
    SELECT
      i.inv_id,
      i.quantity  AS inv_quantity,
      i.reserved  AS inv_reserved,
      i.allocated AS inv_allocated,

      m.inv_location_id,
      m.location,
      m.sublocation,
      m.company_id,
      m.loc_unique_id,

      s.quantity  AS loc_quantity,
      s.reserved  AS loc_reserved,
      s.allocated AS loc_allocated,
      s.expire_date
    FROM inventory i
    JOIN inv_location_stock s
      ON s.inv_id = i.inv_id
    JOIN inv_location_mapping m
      ON m.inv_location_id = s.inv_location_id
    WHERE i.inv_id IN ({placeholders})
    ORDER BY
      s.expire_date IS NULL,
      s.expire_date ASC
    FOR UPDATE;
    """

    cur.execute(sql, inv_ids)
    rows = cur.fetchall()

    # Build inv -> header + ordered locations
    inv_info: Dict[int, dict] = {}
    for r in rows:
        inv_id = int(r["inv_id"])
        if inv_id not in inv_info:
            inv_info[inv_id] = {
                "inv_quantity": r["inv_quantity"],
                "inv_reserved": r["inv_reserved"],
                "inv_allocated": r["inv_allocated"],
                "location_details": [],
            }
        inv_info[inv_id]["location_details"].append({
            "inv_location_id": r["inv_location_id"],
            "loc_quantity": r["loc_quantity"],
            "loc_reserved": r["loc_reserved"],
            "loc_allocated": r["loc_allocated"],
            "expire_date": r["expire_date"],
            "location": r["location"],
            "sublocation": r["sublocation"],
            "company_id": r["company_id"],
            "loc_unique_id": r["loc_unique_id"],
        })

    failures: List[dict] = []
    available_for_reservation: Dict[int, List[dict]] = {}

    for line in inv_lines:
        inv_id = int(line["inv_id"])
        need_total = int(line["qty"])

        r = inv_info.get(inv_id)
        if r is None:
            failures.append({"inv_id": inv_id, "need": need_total, "available": 0, "reason": "missing_stock_row"})
            continue

        available_total = int(r["inv_quantity"]) - int(r["inv_reserved"]) - int(r["inv_allocated"])
        if available_total < need_total:
            failures.append({
                "inv_id": inv_id,
                "need": need_total,
                "available": available_total,
                "quantity": int(r["inv_quantity"]),
                "reserved": int(r["inv_reserved"]),
                "allocated": int(r["inv_allocated"]),
                "reason": "insufficient_total",
            })
            continue

        remaining = need_total
        picks: List[dict] = []

        for loc in r["location_details"]:  # already ordered by expire_date
            loc_available = int(loc["loc_quantity"]) - int(loc["loc_reserved"]) - int(loc["loc_allocated"])
            if loc_available <= 0:
                continue

            take = min(loc_available, remaining)
            picks.append({
                "inv_location_id": int(loc["inv_location_id"]),
                "reserve": take,
                "location_details": loc,
            })
            remaining -= take
            if remaining == 0:
                break

        if remaining != 0:
            failures.append({
                "inv_id": inv_id,
                "need": need_total,
                "available": available_total,
                "reason": "loc_split_failed",
            })
            continue

        available_for_reservation[inv_id] = { "reservations" : picks, "company_id" : int(line["company_id"]) }

    return (len(failures) == 0), failures, available_for_reservation


