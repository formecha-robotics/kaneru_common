#!/usr/bin/env python3
import subprocess
from pathlib import Path
import re
from typing import TextIO, Optional, Dict, List, Any
import json


prefectures = [
    "Hokkaido",
    "Aomori",
    "Iwate",
    "Miyagi",
    "Akita",
    "Yamagata",
    "Fukushima",
    "Ibaraki",
    "Tochigi",
    "Gunma",
    "Saitama",
    "Chiba",
    "Tokyo",
    "Kanagawa",
    "Niigata",
    "Toyama",
    "Ishikawa",
    "Fukui",
    "Yamanashi",
    "Nagano",
    "Gifu",
    "Shizuoka",
    "Aichi",
    "Mie",
    "Shiga",
    "Kyoto",
    "Osaka",
    "Hyogo",
    "Nara",
    "Wakayama",
    "Tottori",
    "Shimane",
    "Okayama",
    "Hiroshima",
    "Yamaguchi",
    "Tokushima",
    "Kagawa",
    "Ehime",
    "Kochi",
    "Fukuoka",
    "Saga",
    "Nagasaki",
    "Kumamoto",
    "Oita",
    "Miyazaki",
    "Kagoshima",
    "Okinawa",
]

TITLE_RE = re.compile(r"<title>\s*(.*?)\s*</title>", re.IGNORECASE | re.DOTALL)

FROM_PARENS_RE = re.compile(r"\(\s*from\s+([^)]+?)\s*\)", re.IGNORECASE)


BASE = "https://www.post.japanpost.jp/service/you_pack/charge/ichiran/{:02d}_en.html"
OUT_DIR = Path("./tmp/")  # keep it tidy



#TABLE_RE = re.compile(
#    r'<table[^>]*class="[^"]*\bdata\b[^"]*\bsp-t15\b[^"]*"[^>]*>(.*?)</table>',
#    re.IGNORECASE | re.DOTALL,
#)

#TH_RE = re.compile(
#    r'<th[^>]*class="[^"]*\bh1\b[^"]*"[^>]*>(.*?)</th>',
#    re.IGNORECASE | re.DOTALL,
#)

DIV_RE = re.compile(r"<div[^>]*>(.*?)</div>", re.IGNORECASE | re.DOTALL)
SMALL_RE = re.compile(r"<small[^>]*>(.*?)</small>", re.IGNORECASE | re.DOTALL)

#ROW_RE = re.compile(
#    r"<tr>\s*<td>\s*(\d+)\s*size\s*</td>\s*"
#    r'<td[^>]*class="[^"]*\bfee\b[^"]*"[^>]*>\s*([0-9,]+)',
#    re.IGNORECASE | re.DOTALL,
#)

#TAG_STRIP_RE = re.compile(r"<[^>]+>")



# Match any table and capture class + body, allowing ' or "
TABLE_ANY_RE = re.compile(
    r"<table\b[^>]*\bclass\s*=\s*(['\"])(?P<class>[^'\"]*)\1[^>]*>(?P<body>.*?)</table>",
    re.IGNORECASE | re.DOTALL,
)

TH_RE = re.compile(
    r"<th\b[^>]*\bclass\s*=\s*(['\"])(?P<class>[^'\"]*)\1[^>]*>(?P<th>.*?)</th>",
    re.IGNORECASE | re.DOTALL,
)

ROW_RE = re.compile(
    r"<tr>\s*<td>\s*(\d+)\s*size\s*</td>\s*"
    r"<td\b[^>]*\bclass\s*=\s*(['\"])(?P<class>[^'\"]*)\2[^>]*>\s*([0-9,]+)",
    re.IGNORECASE | re.DOTALL,
)

TAG_STRIP_RE = re.compile(r"<[^>]+>")

def _strip_tags(s: str) -> str:
    s = TAG_STRIP_RE.sub(" ", s)
    return " ".join(s.split()).strip()

def extract_fee_for_destination(destination: str, fd: TextIO) -> Dict[int, int]:
    """
    Find the fee table whose header (<th class="h1">...) mentions `destination`
    (substring match, case-insensitive), then return {size: fee}.
    """
    dest_key = destination.strip().casefold()
    if not dest_key:
        raise ValueError("destination is empty")

    html = fd.read()

    for tm in TABLE_ANY_RE.finditer(html):
        class_attr = (tm.group("class") or "")
        class_cf = class_attr.casefold()

        # only tables we care about
        if "data" not in class_cf or "sp-t15" not in class_cf:
            continue

        table_body = tm.group("body")

        thm = TH_RE.search(table_body)
        if not thm:
            continue

        th_class = (thm.group("class") or "").casefold()
        if "h1" not in th_class:
            continue

        header_text = _strip_tags(thm.group("th")).casefold()

        # This is the key robustness change:
        # Matches "Hokkaido", "Hokkaido, Tohoku", "Hokkaido / Tohoku", etc.
        if dest_key not in header_text:
            continue

        fees: Dict[int, int] = {}
        for size_s, fee_s in re.findall(r"<tr><td>\s*(\d+)\s*size</td><td[^>]*\bfee\b[^>]*>\s*([0-9,]+)", table_body, re.IGNORECASE):
            fees[int(size_s)] = int(fee_s.replace(",", ""))

        # If you prefer using ROW_RE instead of the simpler findall above, we can switch.
        if not fees:
            # fallback using ROW_RE (covers more formatting variants)
            for rm in ROW_RE.finditer(table_body):
                td_class = (rm.group("class") or "").casefold()
                if "fee" not in td_class:
                    continue
                size = int(rm.group(1))
                fee = int(rm.group(4).replace(",", ""))
                fees[size] = fee

        if not fees:
            raise ValueError(f"Matched destination {destination!r} but found no fee rows")

        return fees

    raise ValueError(f"Destination {destination!r} not found in any fee table")

def _dest_names_from_th(th_html: str) -> List[str]:
    """
    Returns all destination names represented by the <th class="h1"> block.
    Example 1: "Hokkaido" -> ["Hokkaido"]
    Example 2: "<div>Tohoku</div><small> Aomori ...</small>" -> ["Tohoku","Aomori",...]
    """
    th_html = th_html.strip()

    names: List[str] = []

    # Prefer <div> label if present; else use stripped text without <small>
    divm = DIV_RE.search(th_html)
    if divm:
        main = _strip_tags(divm.group(1))
        if main:
            names.append(main)
    else:
        # Remove <small> before stripping, so main header isn't polluted
        th_wo_small = SMALL_RE.sub(" ", th_html)
        main = _strip_tags(th_wo_small)
        if main:
            names.append(main)

    # Add prefectures listed in <small> (space-separated)
    sm = SMALL_RE.search(th_html)
    if sm:
        small_text = _strip_tags(sm.group(1))
        # The Japan Post pages use space-separated prefecture names in English
        for token in small_text.split():
            if token:
                names.append(token)

    # de-dup preserving order
    seen = set()
    out = []
    for n in names:
        key = n.casefold()
        if key not in seen:
            seen.add(key)
            out.append(n)
    return out

"""
def extract_fee_for_destination(destination: str, fd: TextIO) -> Dict[int, int]:
    
    dest_key = destination.strip().casefold()
    if not dest_key:
        raise ValueError("destination is empty")

    html = fd.read()

    for table_html in TABLE_RE.findall(html):
        thm = TH_RE.search(table_html)
        if not thm:
            continue

        dest_names = _dest_names_from_th(thm.group(1))
        if dest_key not in {d.casefold() for d in dest_names}:
            continue

        fees: Dict[int, int] = {}
        for size_s, fee_s in ROW_RE.findall(table_html):
            size = int(size_s)
            fee = int(fee_s.replace(",", ""))
            fees[size] = fee

        if not fees:
            raise ValueError(f"Matched destination {destination!r} but found no fee rows")

        return fees

    raise ValueError(f"Destination {destination!r} not found in any fee table")
"""

def fetch_one(n: int) -> None:
    url = BASE.format(n)
    out_file = OUT_DIR / f"{n}.txt"

    # -L follow redirects
    # --fail makes non-2xx exit non-zero
    # --retry retries transient failures
    # --connect-timeout/--max-time prevent hanging
    cmd = [
        "curl",
        "-L",
        "--fail",
        "--retry", "3",
        "--retry-delay", "1",
        "--connect-timeout", "10",
        "--max-time", "30",
        "-H", "Accept: text/html,application/xhtml+xml",
        "-A", "Mozilla/5.0 (X11; Linux x86_64) KaneruRateScraper/1.0",
        url,
        "-o", str(out_file),
    ]

    subprocess.run(cmd, check=True)

def loop_sources(pages_dir: str = "youpack_pages"):
    pages = OUT_DIR
    for n in range(1, 48):
        path = pages / f"{n}.txt"
        local = ""
        data = {} 
        with path.open("r", encoding="utf-8", errors="replace") as f:
            local = extract_source_location_from_fd(f)
        with Path(f"{path}").open("r", encoding="utf-8", errors="replace") as f:
            local_rates = extract_fee_for_destination(local, f)
            data["origin"] = local
            data["zones"] = {}
            data["zones"]["local"] = local_rates 
        for dest in prefectures:
            if dest == local:
                continue
            with Path(f"{path}").open("r", encoding="utf-8", errors="replace") as f:
                dest_fees = extract_fee_for_destination(dest, f)
                data["zones"][dest] = dest_fees 

        dump_prefecture_json("./domestic/japan/yupack/", local, data)

def extract_source_location_from_fd(fd: TextIO) -> str:
    """
    Reads HTML from fd and extracts source location from the <title> tag,
    e.g. '... (from Hokkaido); Japan Post' -> 'Hokkaido'
    Raises ValueError if it can't find it.
    """
    html = fd.read()

    m = TITLE_RE.search(html)
    if not m:
        raise ValueError("No <title> tag found")

    title_text = m.group(1).strip()

    m2 = FROM_PARENS_RE.search(title_text)
    if not m2:
        raise ValueError(f"Couldn't parse source location from title: {title_text!r}")

    return m2.group(1).strip()


def dump_prefecture_json(
    dest_path: str,
    prefecture_name: str,
    data_dict: Dict[Any, Any],
) -> Path:
    """
    Writes data_dict to:
        dest_path/prefecture_name.json

    Returns the Path written.
    """

    out_dir = Path(dest_path)
    out_dir.mkdir(parents=True, exist_ok=True)

    # sanitize filename just in case
    safe_name = prefecture_name.strip().replace(" ", "_")

    out_file = out_dir / f"{safe_name}.json"

    with out_file.open("w", encoding="utf-8") as f:
        json.dump(data_dict, f, indent=2, ensure_ascii=False)

    return out_file

def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    for n in range(1, 48):
        print(f"[{n:02d}/47] fetching…", end=" ", flush=True)
        fetch_one(n)
        print("ok")

    loop_sources()
        

if __name__ == "__main__":
    main()
