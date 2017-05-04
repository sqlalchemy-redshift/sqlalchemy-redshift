#!/usr/bin/env sh

## Scrapes a list of Redshift reserved words from AWS docs
## and formats for copying and pasting into Python code.

## This was developed on macOS and may not directly translate to
## other platforms.

curl "https://docs.aws.amazon.com/redshift/latest/dg/r_pg_keywords.html" \
    | tr '\n' '\r' \
    | sed 's/.*\(AES128.*WITHOUT\).*/\1/' \
    | tr '\r' '\n' \
    | sed 's/^\([A-Z0-9_]*\).*/"\1",/' \
    | paste -s -d' ' - \
    | fold -s -w 70 \
    | awk '{print "    "$0}' \
    | tr 'A-Z' 'a-z'
