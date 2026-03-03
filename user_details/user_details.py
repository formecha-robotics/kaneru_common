import random
import string
import mysql.connector
import re
from typing import Tuple, Dict, List
import production.inventory_database as db
from production.credentials import db_credentials


CHARSET = string.ascii_lowercase + string.digits   # 36 chars

def update_preferences_db(user_id, lang_code):
    connection = mysql.connector.connect(**db_credentials)
    cursor = connection.cursor()

    sql = """
        INSERT INTO kaneru_user_preferences (user_id, lang_code)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE
            lang_code = VALUES(lang_code)
    """

    try:
        cursor.execute(sql, (user_id, lang_code))
        connection.commit()
        return True

    except mysql.connector.Error as err:
        print(f"❌ Transaction failed: {err}")
        connection.rollback()
        return False

    finally:
        cursor.close()
        connection.close()



def retrieve_preferences(user_id: str):

    db_query = "SELECT * FROM kaneru_user_preferences WHERE user_id = %s"
    results = db.execute_query(db_query, (user_id,))
    
    if len(results) == 0:
        return None
    else:
        return results[0]


def retrieve_all(user_id: str):

    db_query = """
               SELECT d.*, p.lang_code
               FROM kaneru_user_details d
               LEFT JOIN kaneru_user_preferences p
               ON p.user_id = d.user_id
               WHERE d.user_id = %s;
               """
    results = db.execute_query(db_query, (user_id,))
    
    print(results)
    
    if len(results) == 0:
        return None
    else:
        return results[0]


def validate_username(
    username: str,
    user_location_language: str,
    forbidden_list: Dict[str, Dict[str, List[str]]]
) -> Tuple[bool, str]:
    """
    Returns (is_valid, reason)

    Hard rules:
        - Reject if username contains any substring from GLOBAL["substrings"]
        - Reject if username exactly matches any GLOBAL["exact"]
        - Reject if username matches any language-specific exact/substrings

    Soft rules:
        - Allow everything else (may be flagged later by review system)
    """

    # Normalize username for comparison
    uname_norm = username.lower().strip()

    # 1. Global forbidden rules (strict)
    global_exact = forbidden_list.get("GLOBAL", {}).get("exact", [])
    global_sub = forbidden_list.get("GLOBAL", {}).get("substrings", [])

    # Exact global bans
    if uname_norm in global_exact:
        return False, f"Username '{username}' is globally prohibited."

    # Substring global bans
    for bad in global_sub:
        if bad in uname_norm:
            return False, f"Username contains a globally prohibited term: '{bad}'."

    # 2. Language/region specific rules
    lang_rules = forbidden_list.get(user_location_language, {})

    # Language exact bans
    for bad_exact in lang_rules.get("exact", []):
        if uname_norm == bad_exact:
            return False, f"Username '{username}' is prohibited in your language/region."

    # Language substring bans
    for bad_sub in lang_rules.get("substrings", []):
        if bad_sub in uname_norm:
            return False, (
                f"Username contains a prohibited term ('{bad_sub}') "
                f"for your language/region."
            )

    # Passed all checks
    return True, "OK"





