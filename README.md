# Yahoo Gemini Tap

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from [Yahoo Gemini reporting API](http://example.com)
- Extracts the following [reporting cubes](https://developer.yahoo.com/nativeandsearch/guide/reporting/cubes/):
  - [Performance stats](https://developer.yahoo.com/nativeandsearch/guide/reporting/cubes/#performance-stats)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

---

Copyright &copy; 2019 My Pension Expert Limited