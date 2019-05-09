# Yahoo Gemini Tap

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from [Yahoo Gemini reporting API](https://developer.yahoo.com/nativeandsearch/guide/reporting/)
- Extracts the following [reporting cubes](https://developer.yahoo.com/nativeandsearch/guide/reporting/cubes/):
    - performance_stats
    - keyword_stats
    - search_stats
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Connecting

### Requirements

To set up `tap-gemini` in Stitch, you need:

_For each requirement:_
-  **The requirement**. Brief explanation of the requirement. Include links to if relevant.

### Setup

The steps necessary to set up the tap, including instructions for obtaining API credentials, configuring account settings, granting user permissions, etc. if necessary.

### Usage

```bash
python .
```

## Replication

If pertinent, include details about how the tap replicates data and/or uses the API. As Stitch users are billed for total rows replicated, any info that can shed light on the number of rows replicated or reduce usage is considered necessary.

Examples:

- Replication strategy - attribution/conversion windows ([Google AdWords](https://www.stitchdata.com/docs/integrations/saas/google-adwords#data-extraction-conversion-window)), event-based updates, etc.
- API usage, especially for services that enforce rate limits or quotas, like Salesforce or [Marketo](https://www.stitchdata.com/docs/integrations/saas/marketo#marketo-daily-api-call-limits)

## Table Schemas

For **each** table that the tap produces, provide the following:

- Table name: 
- Description:
- Primary key column(s): 
- Replicated fully or incrementally _(uses a bookmark to maintain state)_:
- Bookmark column(s): _(if replicated incrementally)_ 
- Link to API endpoint documentation:

---

## Troubleshooting / Other Important Info

All dates and times use the `advertiser` time zone.

One can debug the API HTTP connection by running the following command:

```bash
python tap_gemini\transport.py --debug
```

---

Copyright &copy; 2019 Stitch
