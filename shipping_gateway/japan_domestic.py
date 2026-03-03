import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Size brackets (cm, total of h+w+d)
# ---------------------------------------------------------------------------
_SIZE_BRACKETS = [60, 80, 100, 120, 140, 160, 170]

_DATA_DIR = os.getenv(
    "YUPACK_DATA_DIR",
    str(Path(__file__).parent / "domestic" / "japan" / "yupack"),
)
_FLATRATE_DATA_FILE = os.getenv(
    "FLATRATE_DATA_FILE",
    str(Path(_DATA_DIR).parent / "japan_post_flatrate.json"),
)

ShippingMethod = Literal[
    "yupacket",
    "letterpack_light",
    "letterpack_plus",
    "yupack",
    "yumail",
]

_FLAT_RATE_SERVICES = frozenset({"letterpack_light", "yupacket", "yumail"})

# ---------------------------------------------------------------------------
# In-process rate caches  (loaded at startup)
# ---------------------------------------------------------------------------
_YUPACK_CACHE: Dict[str, dict] = {}
_FLATRATE_CACHE: Dict[str, Any] = {}


def preload_all_yupack_rates(base_dir: str = _DATA_DIR) -> None:
    """
    Load every *.json rate file in base_dir into the in-process cache.
    Call once at Flask startup so missing/malformed files fail fast.
    """
    p = Path(base_dir)
    if not p.is_dir():
        raise FileNotFoundError(f"Yu-Pack rate directory not found: {p}")

    loaded = 0
    for json_file in sorted(p.glob("*.json")):
        origin = json_file.stem          # e.g. "Tokyo", "Okinawa"
        _load_yupack_origin_table(origin, base_dir)
        loaded += 1

    log.info("Yu-Pack rates preloaded: %d prefecture files from %s", loaded, base_dir)


def preload_flatrate_rates(path: str = _FLATRATE_DATA_FILE) -> None:
    """
    Load japan_post_flatrate.json into the in-process cache.
    Call once at Flask startup so missing/malformed files fail fast.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Flat-rate data file not found: {p}")

    with p.open("r", encoding="utf-8") as f:
        data = json.load(f)

    required = {"letterpack_light", "yupacket", "yumail"}
    missing = required - data.keys()
    if missing:
        raise ValueError(f"Flat-rate data missing service keys: {missing}")

    _FLATRATE_CACHE.update(data)
    log.info("Flat-rate rates preloaded from %s", path)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _load_yupack_origin_table(origin_pref: str, base_dir: str) -> dict:
    p = Path(base_dir) / f"{origin_pref}.json"
    key = str(p.resolve())

    if key in _YUPACK_CACHE:
        return _YUPACK_CACHE[key]

    if not p.exists():
        raise FileNotFoundError(f"Yu-Pack rate file not found: {p}")

    with p.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if "zones" not in data or "origin" not in data:
        raise ValueError(f"Invalid Yu-Pack JSON structure in: {p}")

    _YUPACK_CACHE[key] = data
    log.debug("Loaded Yu-Pack rates for origin '%s'", origin_pref)
    return data


def _size_category(h: float, w: float, d: float) -> int:
    total = h + w + d
    for b in _SIZE_BRACKETS:
        if total <= b:
            return b
    raise ValueError(
        f"Package exceeds 170 cm total size limit (got {total:.1f} cm)"
    )


def _get_yupack_fee(data: dict, origin_pref: str, dest_pref: str, size: int) -> int:
    zones = data.get("zones", {})
    zone_key = "local" if origin_pref == dest_pref else dest_pref
    zone = zones.get(zone_key)
    if zone is None:
        raise ValueError(f"No zone '{zone_key}' in rates for origin '{origin_pref}'")
    fee = zone.get(str(size))
    if fee is None:
        raise ValueError(
            f"No fee for size {size} in zone '{zone_key}' (origin '{origin_pref}')"
        )
    return int(fee)


# ---------------------------------------------------------------------------
# Method eligibility selector
# ---------------------------------------------------------------------------

def best_japanpost_domestic_method(
    weight_g: float,
    height_cm: float,
    width_cm: float,
    depth_cm: float,
) -> ShippingMethod:
    """
    Returns the cheapest eligible Japan Post domestic method for the given
    dimensions/weight based on published constraints.

    Note: to get the actual cheapest price you should also calculate prices
    for each eligible method and compare — this function selects by eligibility
    only, ranked by typical price ordering.
    """
    dims = sorted([height_cm, width_cm, depth_cm], reverse=True)
    length, width, thickness = dims[0], dims[1], dims[2]
    total = length + width + thickness

    # Yu-Packet: total<=60, longest<=34, thickness<=3, weight<=1kg
    if weight_g <= 1000 and total <= 60 and length <= 34 and thickness <= 3:
        return "yupacket"

    # Letter Pack: A4 footprint, weight<=4kg
    if weight_g <= 4000 and length <= 34 and width <= 25:
        if thickness <= 3:
            return "letterpack_light"
        return "letterpack_plus"

    # Yu-Pack: size<=170, weight<=25kg
    if weight_g <= 25_000 and total <= 170:
        return "yupack"

    raise ValueError("No eligible Japan Post domestic method for given size/weight")


# ---------------------------------------------------------------------------
# Flat-rate service calculators
# ---------------------------------------------------------------------------

def _calc_letterpack_light(
    height_cm: float, width_cm: float, depth_cm: float, weight_g: float, cfg: dict
) -> int:
    """Letter Pack Light: ¥430 flat, A4 footprint, max 3cm thick, max 4kg."""
    c = cfg["constraints"]
    dims = sorted([height_cm, width_cm, depth_cm], reverse=True)
    if dims[0] > c["max_height_cm"] or dims[1] > c["max_width_cm"] or dims[2] > c["max_thickness_cm"]:
        raise ValueError(
            f"Letter Pack Light: dimensions {height_cm}×{width_cm}×{depth_cm} cm exceed "
            f"{c['max_height_cm']}×{c['max_width_cm']}×{c['max_thickness_cm']} cm limit"
        )
    if weight_g > c["max_weight_g"]:
        raise ValueError(
            f"Letter Pack Light: weight {weight_g:.0f} g exceeds {c['max_weight_g']} g limit"
        )
    return int(cfg["price"])


def _calc_yupacket(
    height_cm: float, width_cm: float, depth_cm: float, weight_g: float, cfg: dict
) -> int:
    """Yu-Packet: flat rate by thickness tier, ≤3cm depth, ≤60cm combined, max 1kg."""
    c = cfg["constraints"]
    dims = sorted([height_cm, width_cm, depth_cm], reverse=True)
    thickness = dims[2]  # smallest dimension
    combined  = dims[0] + dims[1] + dims[2]

    if combined > c["max_combined_cm"]:
        raise ValueError(
            f"Yu-Packet: combined dimensions {combined:.1f} cm exceed {c['max_combined_cm']} cm"
        )
    if dims[0] > c["max_longest_cm"]:
        raise ValueError(
            f"Yu-Packet: longest side {dims[0]:.1f} cm exceeds {c['max_longest_cm']} cm"
        )
    if thickness > c["max_thickness_cm"]:
        raise ValueError(
            f"Yu-Packet: thickness {thickness:.1f} cm exceeds {c['max_thickness_cm']} cm"
        )
    if weight_g > c["max_weight_g"]:
        raise ValueError(
            f"Yu-Packet: weight {weight_g:.0f} g exceeds {c['max_weight_g']} g limit"
        )
    for tier in cfg["tiers"]:
        if thickness <= tier["max_thickness_cm"]:
            return int(tier["price"])
    raise ValueError(f"Yu-Packet: thickness {thickness:.1f} cm does not match any pricing tier")


def _calc_yumail(weight_g: float, cfg: dict) -> int:
    """Yu-Mail: flat rate by weight tier, max 1kg, printed materials / media only."""
    c = cfg["constraints"]
    if weight_g > c["max_weight_g"]:
        raise ValueError(
            f"Yu-Mail: weight {weight_g:.0f} g exceeds {c['max_weight_g']} g limit"
        )
    for tier in cfg["tiers"]:
        if weight_g <= tier["max_weight_g"]:
            return int(tier["price"])
    raise ValueError(f"Yu-Mail: weight {weight_g:.0f} g does not match any pricing tier")


# ---------------------------------------------------------------------------
# Single-box quote (low-level, used internally and by the simple route)
# ---------------------------------------------------------------------------

def japanpost_domestic_shipping(
    send_address: Dict[str, Any],
    receive_address: Dict[str, Any],
    weight_g: float,
    height_cm: float,
    width_cm: float,
    depth_cm: float,
    service: str = "yupack",
    options: Optional[Dict[str, Any]] = None,
    *,
    yupack_dir: str = _DATA_DIR,
) -> int:
    """
    Returns shipping cost in yen for a single box.

    Args:
        send_address:    dict with key "source"      (prefecture name)
        receive_address: dict with key "destination" (prefecture name)
        weight_g:        gross weight of the packed box in grams
        height_cm / width_cm / depth_cm: external box dimensions
        service:         currently only "yupack"
        options:         optional add-ons, e.g. {"insurance": True, "insurance_value": 50000}
        yupack_dir:      directory containing per-origin JSON rate files
    """
    if service in _FLAT_RATE_SERVICES:
        if not _FLATRATE_CACHE:
            raise RuntimeError(
                "Flat-rate rates not loaded; call preload_flatrate_rates() at startup"
            )
        if service == "letterpack_light":
            return _calc_letterpack_light(height_cm, width_cm, depth_cm, weight_g, _FLATRATE_CACHE["letterpack_light"])
        if service == "yupacket":
            return _calc_yupacket(height_cm, width_cm, depth_cm, weight_g, _FLATRATE_CACHE["yupacket"])
        if service == "yumail":
            return _calc_yumail(weight_g, _FLATRATE_CACHE["yumail"])

    if service != "yupack":
        raise NotImplementedError(f"Service '{service}' is not yet supported")

    if weight_g > 25_000:
        raise ValueError("Yu-Pack weight limit is 25 kg")

    origin_pref = (send_address.get("source") or "").strip().lower().capitalize()
    dest_pref   = (receive_address.get("destination") or "").strip().lower().capitalize()

    if not origin_pref or not dest_pref:
        raise ValueError("Missing prefecture in send_address or receive_address")

    size = _size_category(height_cm, width_cm, depth_cm)
    data = _load_yupack_origin_table(origin_pref, yupack_dir)
    cost = _get_yupack_fee(data, origin_pref, dest_pref, size)

    # Insurance — Yu-Pack includes up to ¥30,000 coverage by default.
    # An additional premium applies above that (flat fee per ¥100,000 band).
    # Placeholder: real premium table to be added when scraped.
    if options and options.get("insurance"):
        insured_value = int(options.get("insurance_value", 0))
        if insured_value > 30_000:
            log.warning(
                "Insurance premium calculation not yet implemented "
                "(insured_value=%d). Returning base cost only.", insured_value
            )

    return cost


# ---------------------------------------------------------------------------
# Box selection + multi-box quote
# ---------------------------------------------------------------------------

def _packing_efficiency() -> float:
    """Conservative packing efficiency factor (accounts for void fill, irregular shapes)."""
    return 0.85


def _box_can_fit_item(
    box: Dict[str, Any],
    item: Dict[str, Any],
    current_weight_g: float,
    current_volume_cm3: float,
) -> bool:
    """
    Returns True if the item can physically fit into the box given what is
    already packed.  Uses a simple volume heuristic with a packing efficiency
    factor rather than true 3-D placement.
    """
    box_dims  = sorted([box["height_cm"], box["width_cm"], box["depth_cm"]], reverse=True)
    item_dims = sorted([item["height_cm"], item["width_cm"], item["depth_cm"]], reverse=True)

    # Item must fit inside box along every axis
    for b, i in zip(box_dims, item_dims):
        if i > b:
            return False

    # Weight check
    if current_weight_g + item["weight_g"] * item.get("quantity", 1) > box.get("max_weight_g", float("inf")):
        return False

    # Volume check (with packing efficiency)
    item_vol  = item["height_cm"] * item["width_cm"] * item["depth_cm"] * item.get("quantity", 1)
    box_vol   = box["height_cm"] * box["width_cm"] * box["depth_cm"] * _packing_efficiency()
    if current_volume_cm3 + item_vol > box_vol:
        return False

    return True


def select_boxes_for_items(
    items: List[Dict[str, Any]],
    available_boxes: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Greedy first-fit-decreasing bin packing.

    Algorithm:
      1. Sort items largest-first (by volume × quantity).
      2. Sort boxes smallest-first so we always try the cheapest fit.
      3. For each item, try to fit it into an already-opened box.
         If it fits nowhere, open the smallest box that can accommodate it.
      4. If no single box can hold the item, raise ValueError.

    Returns a list of packed-box dicts:
      {
        "box":        <box definition from available_boxes>,
        "items":      [{"inv_id": ..., "quantity": ...}, ...],
        "weight_g":   <total gross weight in grams>,
        "volume_cm3": <total item volume in cm³>
      }
    """
    if not items:
        raise ValueError("Item list is empty")
    if not available_boxes:
        raise ValueError("No boxes available")

    # Sort items largest-first by volume (× quantity)
    def item_volume(it: Dict[str, Any]) -> float:
        return it["height_cm"] * it["width_cm"] * it["depth_cm"] * it.get("quantity", 1)

    sorted_items = sorted(items, key=item_volume, reverse=True)

    # Sort boxes smallest-first by volume
    def box_volume(b: Dict[str, Any]) -> float:
        return b["height_cm"] * b["width_cm"] * b["depth_cm"]

    sorted_boxes = sorted(available_boxes, key=box_volume)

    packed: List[Dict[str, Any]] = []   # list of open bins

    for item in sorted_items:
        placed = False

        # Try fitting into an already-open bin
        for bin_ in packed:
            if _box_can_fit_item(bin_["box"], item, bin_["weight_g"], bin_["volume_cm3"]):
                bin_["items"].append({"inv_id": item["inv_id"], "quantity": item.get("quantity", 1)})
                bin_["weight_g"]   += item["weight_g"] * item.get("quantity", 1)
                bin_["volume_cm3"] += item_volume(item)
                placed = True
                break

        if not placed:
            # Open a new bin — find the smallest box that can hold this item alone
            opened = False
            for box in sorted_boxes:
                trial_bin = {"box": box, "items": [], "weight_g": 0.0, "volume_cm3": 0.0}
                if _box_can_fit_item(box, item, 0.0, 0.0):
                    trial_bin["items"].append({"inv_id": item["inv_id"], "quantity": item.get("quantity", 1)})
                    trial_bin["weight_g"]   = item["weight_g"] * item.get("quantity", 1)
                    trial_bin["volume_cm3"] = item_volume(item)
                    packed.append(trial_bin)
                    opened = True
                    break

            if not opened:
                raise ValueError(
                    f"Item '{item['inv_id']}' (dims: "
                    f"{item['height_cm']}×{item['width_cm']}×{item['depth_cm']} cm, "
                    f"{item['weight_g']} g) does not fit in any available box"
                )

    return packed


# ---------------------------------------------------------------------------
# Shipping class compatibility grouping
# ---------------------------------------------------------------------------

_COMPAT_GROUP: Dict[str, str] = {
    "standard":   "standard",
    "fragile":    "standard",   # fragile packs with standard
    "flammable":  "flammable",
    "perishable": "perishable",
}

VALID_SHIPPING_CLASSES = frozenset(_COMPAT_GROUP.keys())


def _group_items_by_compatibility(
    items: List[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    """Split items into compatibility groups. fragile packs with standard."""
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for item in items:
        cls = (item.get("shipping_class") or "standard").strip().lower()
        if cls not in VALID_SHIPPING_CLASSES:
            raise ValueError(
                f"Unknown shipping_class '{cls}' for item {item['inv_id']}"
            )
        group = _COMPAT_GROUP[cls]
        groups.setdefault(group, []).append(item)
    return groups


# ---------------------------------------------------------------------------
# Packing modes
# ---------------------------------------------------------------------------

def _bounding_cuboid(
    items: List[Dict[str, Any]],
) -> Tuple[float, float, float, float]:
    """
    Conservative bounding cuboid for a group of items.
      height = sum of item heights × quantities  (stacking direction)
      width  = max item width
      depth  = max item depth
    Returns (height, width, depth, total_weight_g).
    """
    h      = sum(it["height_cm"] * it.get("quantity", 1) for it in items)
    w      = max(it["width_cm"]  for it in items)
    d      = max(it["depth_cm"]  for it in items)
    weight = sum(it["weight_g"]  * it.get("quantity", 1) for it in items)
    return h, w, d, weight


def _pack_group_envelope(
    items: List[Dict[str, Any]],
    packing_spec: Dict[str, Any],
) -> Tuple[Optional[List[Dict[str, Any]]], List[Dict[str, Any]]]:
    """
    Attempt to pack items as a single envelope.
    Returns (parcels, warnings). parcels is None if items don't fit —
    the caller should auto-upgrade to parcel.
    """
    max_h = float(packing_spec.get("max_height_cm",    34))
    max_w = float(packing_spec.get("max_width_cm",     25))
    max_t = float(packing_spec.get("max_thickness_cm",  3))
    packaging_weight_g = float(packing_spec.get("packaging_weight_g", 0))

    h, w, d, items_weight = _bounding_cuboid(items)
    total_weight = items_weight + packaging_weight_g

    if h > max_h or w > max_w or d > max_t:
        return None, [{
            "code": "PACKING_UPGRADED",
            "message": (
                f"Packing upgraded from envelope to parcel: stacked dimensions "
                f"{h:.1f}×{w:.1f}×{d:.1f} cm exceed envelope limits "
                f"{max_h}×{max_w}×{max_t} cm."
            ),
        }]

    return [{
        "packing_type":  "envelope",
        "box_id":        None,
        "weight_g":      total_weight,
        "dimensions_cm": [h, w, d],
        "items": [
            {"inv_id": it["inv_id"], "quantity": it.get("quantity", 1)}
            for it in items
        ],
    }], []


def _pack_group_parcel(
    items: List[Dict[str, Any]],
    available_boxes: Optional[List[Dict[str, Any]]],
    packaging_weight_g: float = 0,
) -> Tuple[Optional[List[Dict[str, Any]]], List[Dict[str, Any]]]:
    """
    Pack items as a single bounding-cuboid parcel.
    Returns (parcels, warnings). parcels is None if carrier limits exceeded
    and boxes are available — the caller should auto-upgrade to box.
    Raises ValueError if limits exceeded and no boxes available.
    """
    h, w, d, items_weight = _bounding_cuboid(items)
    total_weight = items_weight + packaging_weight_g

    if total_weight > 25_000 or h + w + d > 170:
        if available_boxes:
            return None, [{
                "code": "PACKING_UPGRADED",
                "message": (
                    f"Packing upgraded from parcel to box: bounding cuboid "
                    f"{h:.1f}×{w:.1f}×{d:.1f} cm ({h + w + d:.1f} cm combined), "
                    f"{total_weight:.0f} g exceeds Yu-Pack limits (170 cm combined, 25 kg)."
                ),
            }]
        raise ValueError(
            f"Parcel bounding cuboid {h:.1f}×{w:.1f}×{d:.1f} cm "
            f"({h + w + d:.1f} cm combined), {total_weight:.0f} g exceeds carrier limits "
            f"(170 cm combined, 25 kg). No boxes provided to split into."
        )

    return [{
        "packing_type":  "parcel",
        "box_id":        None,
        "weight_g":      total_weight,
        "dimensions_cm": [h, w, d],
        "items": [
            {"inv_id": it["inv_id"], "quantity": it.get("quantity", 1)}
            for it in items
        ],
    }], []


def _pack_group_box(
    items: List[Dict[str, Any]],
    available_boxes: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Pack items into boxes using FFD bin packing.
    Returns (parcels, warnings).
    Raises ValueError if any item does not fit in any available box.
    """
    packed_bins = select_boxes_for_items(items, available_boxes)

    parcels = []
    for bin_ in packed_bins:
        box = bin_["box"]
        box_weight = float(box.get("weight_g", 0))
        parcels.append({
            "packing_type":  "box",
            "box_id":        box["id"],
            "weight_g":      bin_["weight_g"] + box_weight,
            "dimensions_cm": [box["height_cm"], box["width_cm"], box["depth_cm"]],
            "items":         bin_["items"],
        })
    return parcels, []


def _pack_group(
    items: List[Dict[str, Any]],
    packing_spec: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Pack a compatibility group of items according to packing_spec.
    Applies auto-upgrade logic: envelope → parcel → box.
    Returns (parcels, warnings).
    """
    packing_type       = (packing_spec.get("type") or "box").strip().lower()
    available_boxes    = packing_spec.get("available_boxes")
    packaging_weight_g = float(packing_spec.get("packaging_weight_g", 0))
    warnings: List[Dict[str, Any]] = []

    if packing_type == "envelope":
        parcels, ws = _pack_group_envelope(items, packing_spec)
        warnings.extend(ws)
        if parcels is not None:
            return parcels, warnings
        packing_type = "parcel"                   # auto-upgrade

    if packing_type == "parcel":
        parcels, ws = _pack_group_parcel(items, available_boxes, packaging_weight_g)
        warnings.extend(ws)
        if parcels is not None:
            return parcels, warnings
        packing_type = "box"                      # auto-upgrade  # noqa: F841

    # box
    parcels, ws = _pack_group_box(items, available_boxes)
    warnings.extend(ws)
    return parcels, warnings


# ---------------------------------------------------------------------------
# Basket quote (compatibility grouping + packing + pricing)
# ---------------------------------------------------------------------------

def japanpost_domestic_shipping_basket(
    send_address:    Dict[str, Any],
    receive_address: Dict[str, Any],
    items:           List[Dict[str, Any]],
    packing:         Dict[str, Any],
    service:         str = "yupack",
    options:         Optional[Dict[str, Any]] = None,
    *,
    yupack_dir: str = _DATA_DIR,
) -> Dict[str, Any]:
    """
    Groups items by shipping-class compatibility, packs each group according
    to the packing spec (with auto-upgrade), prices each parcel, and returns
    the full OMS shipment response.

    packing spec variants:
      {"type": "parcel"}
      {"type": "envelope", "max_thickness_cm": 3, "max_width_cm": 25, "max_height_cm": 34}
      {"type": "box", "available_boxes": [...]}
      Parcel mode also accepts "available_boxes" for auto-upgrade to box.
    """
    all_warnings: List[Dict[str, Any]] = []

    groups = _group_items_by_compatibility(items)
    log.info("basket | groups=%s item_count=%d", list(groups.keys()), len(items))

    if len(groups) > 1:
        all_warnings.append({
            "code": "COMPATIBILITY_SPLIT",
            "message": (
                f"Items split into {len(groups)} shipments due to incompatible "
                f"shipping classes: {' and '.join(sorted(groups.keys()))}."
            ),
        })

    if any(
        (it.get("shipping_class") or "standard").strip().lower() == "fragile"
        for it in items
    ):
        all_warnings.append({
            "code": "FRAGILE_ITEMS_PRESENT",
            "message": "One or more items flagged fragile — special handling required.",
        })

    all_parcels: List[Dict[str, Any]] = []
    total_cost = 0

    for group_name, group_items in groups.items():
        parcels, group_warnings = _pack_group(group_items, packing)
        all_warnings.extend(group_warnings)

        for parcel in parcels:
            h, w, d = parcel["dimensions_cm"]
            parcel_cost = japanpost_domestic_shipping(
                send_address,
                receive_address,
                weight_g  = parcel["weight_g"],
                height_cm = h,
                width_cm  = w,
                depth_cm  = d,
                service   = service,
                options   = options,
                yupack_dir = yupack_dir,
            )
            parcel["compatibility_group"] = group_name
            parcel["cost"] = parcel_cost
            total_cost += parcel_cost

        log.info(
            "basket | group=%s packing_type=%s parcels=%d group_cost=%d",
            group_name,
            parcels[0]["packing_type"] if parcels else "none",
            len(parcels),
            sum(p["cost"] for p in parcels),
        )
        all_parcels.extend(parcels)

    return {
        "success":        True,
        "carrier":        "japan_post",
        "service":        service,
        "currency":       "JPY",
        "total_cost":     total_cost,
        "cost_breakdown": {"base_rate": total_cost, "insurance_premium": 0},
        "shipment": {
            "parcels":        all_parcels,
            "total_parcels":  len(all_parcels),
            "total_weight_g": sum(p["weight_g"] for p in all_parcels),
        },
        "warnings":          all_warnings,
        "rates_captured_at": None,
        "calculated_at":     datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
