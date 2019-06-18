# Yahoo Gemini Tap

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

* Pulls raw data from [Yahoo Gemini reporting API](https://developer.yahoo.com/nativeandsearch/guide/reporting/)
* Extracts the [reporting cubes](https://developer.yahoo
.com/nativeandsearch/guide/reporting/cubes/) detailed below
* Outputs the schema for each resource
* Incrementally pulls data based on the input state

# Connecting

## Requirements

To install `tap-gemini` in Stitch, you need to create an API application and generate an OAuth 2.0 
client ID and refresh token are required. See the [authetnication documentation](https://developer.yahoo.com/nativeandsearch/guide/navigate-the-api/authentication/) on the Oath 
website.

## Setup

Enter the client ID as the username and the refresh token as the password.

## Usage

Follow the instructions below to use the tap as a Python packag.

### Installation

Create a virtual environment and install the package using `pip`. These instructions are bash 
commands that will work on Unix-based platforms.

```bash
python3 -m venv ~/.virtualenvs/tap-gemini
source ~/.virtualenvs/tap-gemini/bin/activate
pip install tap-gemini
deactivate
```

### Execution

Run the following command to run the tap.

```bash
~/.virtualenvs/tap-gemini/bin/tap-gemini --config ~/my_config_file.json
```

To output the data to a CSV file, pipe the data stream into [target-csv](https://github.com/singer-io/target-csv):

```bash
~/.virtualenvs/tap-gemini/bin/tap-gemini --config ~/my_config_file.json | ~/
.virtualenvs/target-csv/bin/target-csv
```

## Replication

Each incremental report run begins at the timestamp when books were marked closed (i.e. when no 
further changes to the data are written.)

For historic data loads, the reports will run over the largest possible time frame. Some reports 
have a limited time range as detailed below:

* performance_stats: 15 days
* product_ads: 400 days
* site_performance_stats: 400 days
* keyword_stats: 750 days

## Table Schemas

Most tables have the following primary key columns:

* Advertiser ID
* Day

The table schemas are detailed below.

### Reports

The following reporting cubes are implemented:

* [adjustment_stats](https://developer.yahoo.com/nativeandsearch/guide/reporting/cubes/#product-ad-performance-stats)
    - Description: This cube provides performance metrics for over delivery adjustments for spend, which are not available in other cubes.
    - Primary key columns:
        * Advertiser ID
        * Day
    - Replication: Incremental
    - Bookmark column(s): Day
* [ad_extension_details](https://developer.yahoo.com/nativeandsearch/guide/reporting/cubes/#ad-extension-details)
* [call_extension_stats](https://developer.yahoo.com/nativeandsearch/guide/reporting/cubes/#call-extension-stats)
* [campaign_bid_performance_stats](https://developer.yahoo.com/nativeandsearch/guide/reporting/cubes/#campaign-bid-performance-stats)
* [conversion_rules_stats](https://developer.yahoo.com/nativeandsearch/guide/reporting/cubes/#conversion-rules-stats)
* [domain_performance_stats](https://developer.yahoo.com/nativeandsearch/guide/reporting/cubes/#domain-performance-stats)
* [keyword_stats](https://developer.yahoo.com/nativeandsearch/guide/reporting/cubes/#keyword-stats)
* [performance_stats](https://developer.yahoo.com/nativeandsearch/guide/reporting/cubes/#performance-stats)
    - Description: This cube has performance stats for all levels down to the ad level. It is recommended to use this cube when querying for native ads campaign data. The cube does not include keyword level metrics. Data for both search and native campaigns is provided - you can use the “Source” field to filter for a specific channel. Note that the cube does not include any over delivery spend adjustments which are available in the adjustment_stats cube.
    - Primary key columns:
        * Advertiser ID
        * Day
    - Replication: Incremental
    - Bookmark column: Day
* [product_ads](https://developer.yahoo.com/nativeandsearch/guide/reporting/cubes/#product-ads)
* [product_ad_performance_stats](https://developer.yahoo.com/nativeandsearch/guide/reporting/cubes/#product-ad-performance-stats)
* [search_stats](https://developer.yahoo.com/nativeandsearch/guide/reporting/cubes/#search-stats)
* [site_performance_stats](https://developer.yahoo.com/nativeandsearch/guide/reporting/cubes/#site-performance-stats)
* [slot_performance_stats](https://developer.yahoo.com/nativeandsearch/guide/reporting/cubes/#slot-performance-stats)
* [structured_snippet_extension_stats](https://developer.yahoo.com/nativeandsearch/guide/reporting/cubes/#structured-snippet-extension-stats)
* [user_stats](https://developer.yahoo.com/nativeandsearch/guide/reporting/cubes/#user-stats)

### Objects

The following [account structure objects](https://developer.yahoo.com/nativeandsearch/guide/objects.html) are implemented.

* [advertiser](https://developer.yahoo.com/nativeandsearch/guide/reporting/cubes/#advertiser)
* [campaign](https://developer.yahoo.com/nativeandsearch/guide/reporting/cubes/#campaign)

### Unsupported fields

Some fields have been excluded from the schema (i.e. the meta-data inclusion is set to 
`unsupported`) because they are incompatible with other fields. This could probably be fixed by 
defining meta-data exclusions that depend on other fields.

## Troubleshooting / Other Important Info

All dates and times use the `advertiser` time zone.

---

Copyright &copy; 2019 Stitch
