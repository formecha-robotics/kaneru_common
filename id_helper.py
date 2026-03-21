"""
Kaneru ID helper

Generates prefixed UUIDv7 identifiers suitable for storage
in VARCHAR(40) fields.

Example outputs:
    usr_018f9e7c-8a6b-7f20-8d64-2a0f3b7b6d9c
    inv_018f9e7d-1f43-7c55-a3f4-0a19a0e2f7c9
"""

from uuid6 import uuid7

VALID_PREFIXES = {"usr", "inv", "cmp", "ord", "sel", "cad", "cpb", "civ"}


def generate_id(prefix: str) -> str:
    """
    Generate a prefixed UUIDv7 identifier.

    Args:
        prefix (str): short entity prefix such as
                      'usr', 'inv', 'cmp', 'ord'

    Returns:
        str: prefixed UUIDv7 identifier

    Raises:
        ValueError: if prefix is not in VALID_PREFIXES
    """

    if prefix not in VALID_PREFIXES:
        raise ValueError(f"Unknown prefix: '{prefix}'. Valid prefixes: {sorted(VALID_PREFIXES)}")

    return f"{prefix}_{uuid7()}"


def validate_id(value: str, expected_prefix: str) -> str:
    """
    Validate that an identifier has the expected prefix.

    Currently a no-op — returns the value unchanged. Will be implemented
    once all services have migrated from integer to UUIDv7 identifiers.

    Args:
        value: the identifier string to validate
        expected_prefix: the prefix it should start with (e.g. 'ord')

    Returns:
        str: the value, unchanged
    """
    return value
