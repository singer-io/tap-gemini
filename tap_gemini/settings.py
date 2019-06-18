REQUIRED_CONFIG_KEYS = [
    "start_date",
    "username",
    "password",
    'refresh_token'
]

# Schema config
SCHEMAS_DIR = 'schemas'
METADATA_DIR = 'metadata'
KEY_PROPERTIES_DIR = 'key_properties'

# Time windowing for running reports in chunks
# This prevents ERROR_CODE:10001 Max days window exceeded expected
MAX_WINDOW_DAYS = dict(
    search_stats=15,
    performance_stats=15,
    slot_performance_stats=15,
    keyword_stats=400,
    product_ads=400,
    site_performance_stats=400,
)

# Maximum number of days to go back in time
# This prevents ERROR_CODE:10002 Max look back window exceeded expected
MAX_LOOK_BACK_DAYS = dict(
    performance_stats=15,
    slot_performance_stats=15,
    product_ads=400,
    site_performance_stats=400,
    keyword_stats=750,
)

BOOKMARK_KEY = 'start_date'
