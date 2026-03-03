import re

def sanitize(s: str) -> str:
    """
    Sanitizes an inventory name to be MySQL-safe without being overly restrictive.
    Removes control characters and escapes quotes.
    """
    if not isinstance(s, str):
        return ""

    # Strip out control characters (e.g. newlines, tabs, null bytes)
    s = re.sub(r"[\x00-\x1F\x7F]", "", s)

    # Optionally escape single quotes (if using manual SQL — not needed with parameterized queries)
    s = s.replace("'", "''")

    # Optionally strip leading/trailing whitespace
    return s.strip()


