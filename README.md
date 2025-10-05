# cannibalize

SEO keyword cannibalization detector — multi-signal detection, severity scoring, actionable recommendations.

## What it does

- Detects keyword cannibalization from Google Search Console data
- Scores severity using position volatility, click dilution, and content similarity
- Classifies cases: split authority, wrong page ranking, redundant content, intent mismatch
- Generates actionable recommendations (merge, redirect, differentiate)
- Tracks fixes over time with before/after metrics
- Exports to Excel and CSV

## Install

```bash
uv pip install .
```

Or with pipx:

```bash
pipx install .
```

## Quick start

```bash
# Import GSC data from CSV
cannibalize init
cannibalize ingest csv export.csv

# Run detection
cannibalize crawl
cannibalize detect

# Export results
cannibalize export results.xlsx
```

## License

MIT
