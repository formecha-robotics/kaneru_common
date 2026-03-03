# CLAUDE.md — shipping_gateway

This file is read automatically by Claude Code every session. It contains architecture decisions, conventions, and context for the shipping_gateway service.

---

## Project Overview

`shipping_gateway` is a Python/Flask microservice for calculating shipping costs. It sits at the centre of an e-commerce architecture and is called by:

- A **Next.js frontend** (client-facing, at the point of purchase — needs only price and currency)
- An **Order Management System (OMS)** (internal, needs full box/item mapping for picker instructions)

The service is part of a wider platform called **Kaneru** (tokyobookshelf.com). Other services in the stack follow the same patterns described here.

---

## Architecture Principles

- **Carrier abstraction**: Each carrier is an independent calculator. All share a common interface. Never leak carrier-specific parameters into the gateway layer.
- **Rate data**: Rates are loaded from local JSON files at startup. No live carrier API calls unless unavoidable. Each rate file must include a `captured_at` date. Log a warning if rates are stale.
- **Startup validation**: All rate files are pre-loaded at `create_app()` time. Missing or malformed files must fail loudly at startup, not silently at request time.
- **In-process cache**: Rate data is small and read-only. Use in-process memory (not Redis) unless there is a specific reason to share state across workers.
- **Stateless packing spec**: All packing constraints are passed in the request. The service does not look up box sizes, envelope limits, or carrier constraints from internal config — the caller owns this.

---

## Route Structure

| Route | Caller | Response |
|---|---|---|
| `POST /shipping/domestic` | Next.js frontend | Lean: cost + currency only |
| `POST /shipping/domestic/basket` | OMS | Full: parcel mapping, item assignment, warnings |
| *(future)* `POST /shipping/best` | Both | Best-price/time comparison across carriers |

- Frontend routes return **lean responses** (cost, currency, carrier, service, calculated_at).
- OMS routes return **full responses** (all of the above + parcel configuration, item-to-parcel mapping, warnings, rate capture date).
- Separate routes for the two consumers — do not use a `detail_level` flag.

---

## Request Schema (Basket Route)

```json
{
  "country": "japan",
  "source": "Tokyo",
  "destination": "Okinawa",
  "carrier": "japan_post",
  "service": "yupack",
  "options": {
    "insurance": false,
    "insurance_value": 0
  },
  "packing": {
    "type": "box",
    "available_boxes": [
      { "id": "small",  "height_cm": 20, "width_cm": 15, "depth_cm": 10, "max_weight_g": 5000 },
      { "id": "medium", "height_cm": 35, "width_cm": 25, "depth_cm": 20, "max_weight_g": 10000 }
    ]
  },
  "items": [
    { "inv_id": 1001, "quantity": 2, "weight_g": 400, "height_cm": 12, "width_cm": 8, "depth_cm": 4, "shipping_class": "standard" },
    { "inv_id": 1002, "quantity": 1, "weight_g": 800, "height_cm": 18, "width_cm": 14, "depth_cm": 6, "shipping_class": "fragile" }
  ]
}
```

**Packing type variants:**

```json
"packing": { "type": "parcel" }
```

```json
"packing": {
  "type": "envelope",
  "max_thickness_cm": 3,
  "max_width_cm": 25,
  "max_height_cm": 34
}
```

**Key field notes:**
- `inv_id` is always an **integer**.
- `quantity` represents fungible items with the same `inv_id` (same product, multiple units).
- `shipping_class` is the caller's responsibility — the service does not look it up. Defaults to `standard` if omitted.
- `options` is extensible — add fields here without breaking the schema.

---

## Shipping Classes

Four classes are defined. The caller sets `shipping_class` on each item.

| Class | Packs with |
|---|---|
| `standard` | `standard`, `fragile` |
| `fragile` | `standard`, `fragile` — flagged for special handling in response |
| `flammable` | `flammable` only |
| `perishable` | `perishable` only |

**Compatibility grouping:** Before bin packing, items are split into compatibility groups. Each group is packed and priced independently, producing one or more shipments. The response always explains splits and upgrades via the `warnings` array.

Default `shipping_class` if omitted: `standard`.

---

## Packing Types

Three modes. Auto-upgrade applies within each compatibility group: `envelope → parcel → box`.

### envelope
- Flat items only. All items must fit within the caller-supplied thickness and footprint constraints.
- Stacking: `height` = sum of all item heights × quantities, `width`/`depth` = max across items.
- If any item or stack exceeds the envelope constraints, auto-upgrade to `parcel` and add a warning.
- Envelope constraints are passed in the request (not hardcoded per carrier).

### parcel
- No box list. Conservative bounding cuboid:
  - `height` = sum of all item heights × quantities
  - `width` = max item width
  - `depth` = max item depth
- Validate against carrier limits (Yu-Pack: combined h+w+d ≤ 170cm, weight ≤ 25kg).
- If exceeded and `available_boxes` provided in packing spec, auto-upgrade to `box` and add a warning.
- If no boxes available, return `EXCEEDS_CARRIER_LIMIT`.

### box
- FFD (First-Fit Decreasing) bin packing from `available_boxes`.
- Items sorted largest-first by volume (h × w × d × quantity).
- Boxes tried smallest-first.
- Apply **85% volume efficiency factor** per box (accounts for void fill and irregular shapes).
- Both volume and weight checked per box.
- If a single item exceeds the largest box, return `ITEM_EXCEEDS_MAX_BOX`.

---

## Response Schema (Full / OMS)

```json
{
  "success": true,
  "carrier": "japan_post",
  "service": "yupack",
  "currency": "JPY",
  "total_cost": 3000,
  "cost_breakdown": { "base_rate": 3000, "insurance_premium": 0 },
  "shipment": {
    "parcels": [
      {
        "compatibility_group": "standard",
        "packing_type": "box",
        "box_id": "medium",
        "weight_g": 1600,
        "dimensions_cm": [35, 25, 20],
        "cost": 1500,
        "items": [
          { "inv_id": 1001, "quantity": 2 },
          { "inv_id": 1002, "quantity": 1 }
        ]
      },
      {
        "compatibility_group": "flammable",
        "packing_type": "parcel",
        "box_id": null,
        "weight_g": 400,
        "dimensions_cm": [20, 10, 8],
        "cost": 1500,
        "items": [
          { "inv_id": 1003, "quantity": 1 }
        ]
      }
    ],
    "total_parcels": 2,
    "total_weight_g": 2000
  },
  "warnings": [
    {
      "code": "COMPATIBILITY_SPLIT",
      "message": "Items split into 2 shipments due to incompatible shipping classes: standard/fragile and flammable."
    }
  ],
  "rates_captured_at": "2025-01-15",
  "calculated_at": "2026-02-26T10:00:00Z"
}
```

**Important:** Multiple parcels = multiple shipments, each with its own cost. `total_cost` is the sum across all parcels. The OMS uses the `items` array within each parcel to generate picker instructions.

---

## Warning Codes

Warnings are always returned in the response — never silently swallowed.

| Code | Meaning |
|---|---|
| `COMPATIBILITY_SPLIT` | Items split into multiple shipments due to incompatible shipping classes |
| `PACKING_UPGRADED` | Packing type auto-upgraded (e.g. envelope → parcel) because items didn't fit |
| `RATE_DATA_STALE` | Rate data older than expected threshold |
| `FRAGILE_ITEMS_PRESENT` | One or more items flagged fragile — special handling required |

---

## Error Codes

Use structured error codes — do not make callers parse error strings.

| Code | Meaning |
|---|---|
| `UNSUPPORTED_CARRIER` | Carrier not implemented for this country |
| `UNSUPPORTED_SERVICE` | Service type not available for this carrier |
| `RATE_DATA_MISSING` | Rate file not found for origin |
| `ITEM_EXCEEDS_MAX_BOX` | Single item larger than largest available box |
| `EXCEEDS_CARRIER_LIMIT` | Total shipment exceeds carrier size/weight limits |
| `INVALID_INPUT` | Schema validation failure |

---

## Carrier: Japan Post / Yu-Pack

- **Rate data**: 47 JSON files, one per origin prefecture (e.g., `Okinawa.json`). Located in `YUPACK_DATA_DIR` (env var, defaults to `./domestic/japan/yupack/`).
- **Rate loading**: All 47 files loaded at startup into in-process cache. Key: prefecture name (lowercased, normalized).
- **Rate structure**: Size-based (not weight-based). Size determined by combined dimensions (h + w + d in cm). Weight is also a constraint.
- **Tax**: Japan Post rates are **tax-inclusive (消費税込)**. Note this explicitly in rate files.
- **Insurance**: Yu-Pack includes ¥30,000 coverage by default. Additional coverage available via `options.insurance` and `options.insurance_value`. Premium table to be added.
- **Prefectures**: There are exactly 47 — this will not change.
- **Carrier limits**: Combined h+w+d ≤ 170cm, weight ≤ 25kg per parcel.

---

## Carrier: Japan Post / Flat-Rate Services

Three additional Japan Post domestic services are supported. All are **flat-rate with no origin/destination matrix** — `source` and `destination` fields are ignored for these services and should be noted in the response. Rates live in a single config file `data/japan_post_flatrate.json`, not per-prefecture files.

### Letter Pack Light (`letterpack_light`)
- **Price**: ¥430 flat, nationwide
- **Constraints**: A4 footprint (34 × 25cm), max 3cm thick, max 4kg
- **Delivery**: To mailbox, no signature required, tracking included
- **Options**: No optional services available (no insurance, no signature)
- **Packing type**: Always `envelope` — caller must pass envelope constraints in the request matching these limits
- **Content**: Any items that fit

### Yu-Packet (`yupacket`)
- **Price**: Flat rate by thickness tier, nationwide:
  - ≤1cm → ¥250
  - ≤2cm → ¥310
  - ≤3cm → ¥360
- **Constraints**: Combined h+w+d ≤ 60cm, longest side ≤ 34cm, depth ≤ 3cm, max 1kg
- **Delivery**: To mailbox, tracking included
- **Packing type**: Always `envelope` — thickness determines price tier
- **Content**: Any items that fit

### Yu-Mail (`yumail`)
- **Price**: Flat rate by weight tier, nationwide:
  - ≤150g → ¥190
  - ≤250g → ¥230
  - ≤500g → ¥320
  - ≤1kg → ¥380
- **Constraints**: Max 1kg, standard postal size limits apply
- **Delivery**: To mailbox
- **Content restriction**: Printed materials (books, magazines, pamphlets) and electromagnetic record media (CDs, DVDs) **only**. The caller is responsible for ensuring items qualify. The service does not validate content type.
- **Packing type**: Always `parcel` — weight-based, no box selection needed

### Flat-rate pricing notes
- All three services are **tax-inclusive (消費税込)**, consistent with Yu-Pack rates.
- No `captured_at` staleness concern — rates are hardcoded and only change when Japan Post announces a revision. Update the config file and redeploy when that happens.
- A single `domestic/japan/japan_post_flatrate.json` holds all three rate tables, loaded at startup alongside Yu-Pack files.

---

## Options / Add-ons Pattern

Do **not** create separate service types for add-ons (e.g., `yupack_insurance`). Use the `options` object instead:

```json
"options": {
  "insurance": true,
  "insurance_value": 50000
}
```

Each carrier calculator knows whether insurance is included by default, whether it's available as an option, and how to price it (flat fee vs. percentage of declared value). This pattern generalises to: signature on delivery, refrigeration, fragile handling surcharges, etc.

---

## Authentication

JWT-based internal policy enforcement using the shared `enforce_internal_policy` library. Wired in via `@app.before_request`.

**Controlled by environment variable for local dev:**

```python
if os.getenv("ENABLE_AUTH", "true").lower() == "true":
    @app.before_request
    def enforce_jwt_internal_policy():
        ...
```

Run locally with `ENABLE_AUTH=false`. **Never deploy with auth disabled.**

Call matrix (to be expanded):
```python
AUTH_CALL_MATRIX = {
    ("POST", "/shipping/domestic"): {
        "callers": ["website_tokyobookshelf"],
        "scopes": ["shipping.domestic_quote"],
    },
    ("POST", "/shipping/domestic/basket"): {
        "callers": ["order_gateway"],
        "scopes": ["shipping.domestic_fulfillment"],
    },
}
```

---

## Logging & Observability

Each request must log enough to reconstruct the calculation:
- Input (country, source, destination, carrier, service, item count)
- Compatibility groups and any splits
- Selected packing type per group, and any auto-upgrades
- Item-to-parcel mapping
- Total cost
- Any warnings (stale rates, fragile items, etc.)

This is essential for auditing customer shipping charge disputes.

---

## Environment Variables

All variables are optional. Defaults work out of the box for Docker and local dev.

| Variable | Default | Purpose |
|---|---|---|
| `ENABLE_AUTH` | `true` | Set to `false` for local dev only |
| `YUPACK_DATA_DIR` | `<module_dir>/domestic/japan/yupack/` | Path to Yu-Pack rate JSON files |
| `FLATRATE_DATA_FILE` | `<module_dir>/domestic/japan/japan_post_flatrate.json` | Path to flat-rate JSON file |
| `INTERNAL_ISSUER` | `kaneru-internal` | JWT issuer |
| `JWT_PUBLIC_KEY_PATH` | `None` | Path to key file: line 1 = website_tokyobookshelf, line 2 = order_gateway |

---

## Conventions

- **Python** with **Flask**.
- **inv_id** is always an `int` (never a string).
- Address fields: `source` and `destination` are prefecture name strings for domestic Japan routes.
- Normalise prefecture names: `.strip().lower().capitalize()` before lookup.
- Dimensions always in **cm**, weights always in **grams**.
- Currency always explicit in responses (`"JPY"`).
- Dates: `captured_at` as `YYYY-MM-DD` string; `calculated_at` as ISO 8601 UTC datetime string.
- Do not use `sudo` for anything. Use `~/.npm-global` for global npm packages.

---

## Files

```
shipping_gateway/
├── shipping_gateway.py           # Flask app, routes, input validation
├── japan_domestic.py             # Japan Post calculator: Yu-Pack + flat-rate services + bin packing
├── jwt_config.py                 # Auth call matrix and key config
├── domestic/japan/
│   ├── yupack/                   # 47 JSON rate files, one per origin prefecture
│   │   ├── Okinawa.json
│   │   └── ...
│   └── japan_post_flatrate.json  # Flat-rate tables: letterpack_light, yupacket, yumail
└── CLAUDE.md                     # This file
```

---

## Current Status

- ✅ `/shipping/domestic` — simple domestic quote (single dimensions, no box selection)
- ✅ `/shipping/domestic/basket` — basket route with FFD bin packing (box mode)
- ✅ Yu-Pack rate tables (47 prefectures)
- ✅ Auth (env-gated for dev)
- ✅ Parcel packing mode
- ✅ Envelope packing mode
- ✅ Shipping class compatibility grouping
- ✅ Auto-upgrade logic (envelope → parcel → box)
- ✅ Warnings in response
- ✅ Letter Pack Light (`letterpack_light`) — flat rate ¥430, envelope mode
- ✅ Yu-Packet (`yupacket`) — flat rate by thickness tier, envelope mode
- ✅ Yu-Mail (`yumail`) — flat rate by weight tier, media items only
- 🔲 Insurance premium table
- 🔲 Additional carriers (Yamato, Sagawa, etc.)
- 🔲 International routes
- 🔲 Best-carrier comparison route
