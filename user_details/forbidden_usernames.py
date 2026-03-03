# Example hardcoded list you can expand later
forbidden_list = {
    "GLOBAL": {
        # Exact matches only, full username must equal these
        "exact": [
            "support",
            "root",
            "system",
            "moderator"
        ],

        # Substrings – if these appear anywhere → reject
        # "admin" and "kaneru" always banned
        "substrings": [
            "admin",
            "kaneru"
        ]
    },

    # Language-specific rules (example)
    "en": {
        "exact": [
            "god",
            "jesus"
        ],
        "substrings": [
            "slave"
        ]
    },

    "jp": {
        "exact": [
            "天皇",   # Emperor
        ],
        "substrings": [
            "死",     # death
        ]
    },

    "mn": {
        "exact": [
            "чөтгөр"  # demon/devil
        ],
        "substrings": [
            "там"    # hell
        ]
    }
}

