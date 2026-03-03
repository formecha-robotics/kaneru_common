keys_and_policy = { 
    "BOOK_DESCRIPTIONS" : { "key_prefix" : "book_desc_", "expiry_policy" : 60, "description" : "Store for full book descriptions, but not linked to inventory"},
    "PARTIAL_BOOK_DESCRIPTIONS" : { "key_prefix" : "partial_book_description_", "expiry_policy" : 2880, "description" : "Store for full partial book descriptions, awaiting missing data from client"},
    "LATENT_PRICE" : { "key_prefix" : "latent_price_", "expiry_policy" : 43200, "description" : "Store for latent prices"},
    "EXTERNAL_SEARCH" : { "key_prefix" : "external_search_", "expiry_policy" : 120, "description" : "Store for data from external search, Google Book API, and Open API"},
    "INVENTORY_BOOK_DETAILS" : { "key_prefix" : "inventory_book_", "expiry_policy" : 300, "description" : "Store for book inventory item, details"},
    "FX_RATES" : { "key_prefix" : "daily_fx_rates_", "expiry_policy" : 1440, "description" : "Cache of daily FX rates"},
    "SYSTEM_JOB" : { "key_prefix" : "system_job_", "expiry_policy" : 10080, "description" : "System job details"},
    "CATEGORY_SEARCH" : { "key_prefix" : "search_cat_", "expiry_policy" : 1440, "description" : "caching search by category"},
    "VINTAGE_BUILDER" : { "key_prefix" : "vintage_builder_", "expiry_policy" : 2880, "description" : "used for building vintage book details"},
    "ECOMM_FEATURES" : { "key_prefix" : "ecomm_features_", "expiry_policy" : 1440, "description" : "used for building webpage features"},
    "ECOMM_RECOMMENDATIONS" : { "key_prefix" : "ecomm_recommend_", "expiry_policy" : 2880, "description" : "used for store similar items for recommendations"},
    
}



