#!/usr/bin/env python3
import csv
import re
import sys
from pathlib import Path

csv.field_size_limit(sys.maxsize)

# ---------- configuration ----------
SRC_CSV  = Path("web_scraper/google_scholar_full_abstracts.csv")             # your input file
DEST_CSV = Path("web_scraper/papers_regex_filtered.csv")   # where results will go
ABSTR_COL = "full_abstract"               # column that holds the abstract
# ------------------------------------

# one pattern catches “wait-free”, “wait free”, “lock-free”, “lock free”
free_regex = re.compile(
   r"\b(?:wait|lock|obstruction)[\s\u2011\u2012\u2013\u2014\u2060-]*free\b",
   re.IGNORECASE,
)

with SRC_CSV.open(newline="", encoding="utf-8") as src, \
   DEST_CSV.open("w", newline="", encoding="utf-8") as dest:

   reader = csv.DictReader(src, delimiter=";", quotechar='"')
   writer = csv.DictWriter(dest, fieldnames=reader.fieldnames,
                           delimiter=";", quotechar='"', quoting=csv.QUOTE_MINIMAL)
   writer.writeheader()

   for row in reader:
      abstract = row.get(ABSTR_COL, "")
      match_free = bool(free_regex.search(abstract))
      is_absent  = abstract.strip() == "ABSTRACT_NOT_FOUND"

      if match_free or is_absent:
         writer.writerow(row)

print(f"Done! {DEST_CSV} now contains all papers whose abstract mentions wait-/lock-/obstruction-free")
