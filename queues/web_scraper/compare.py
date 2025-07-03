from __future__ import annotations
import csv
import sys
from pathlib import Path
from typing import Set


def collect_marked_urls(old_path: Path) -> Set[str]:
   """
   Return a set with every `url` for which the *last* field of the row is '1'.
   The header of the old file is slightly malformed, so we use a plain reader.
   """
   with old_path.open(newline="", encoding="utf-8") as f:
      reader = csv.reader(f, delimiter=";")
      header = next(reader, [])                       # first line
      try:
         url_idx = header.index("url")
      except ValueError:
         sys.exit(f"'url' column not found in {old_path}")

      marked: Set[str] = set()
      for row in reader:
         if row and row[-1].strip() == "1":
               marked.add(row[url_idx].strip())
      return marked


def copy_with_marker(new_path: Path, dest_path: Path, marked_urls: Set[str]) -> None:
   with new_path.open(newline="", encoding="utf-8") as fin, \
      dest_path.open("w", newline="", encoding="utf-8") as fout:

      reader = csv.DictReader(fin, delimiter=";")
      fieldnames = reader.fieldnames + ["old_marker"]
      writer = csv.DictWriter(
         fout, fieldnames=fieldnames,
         delimiter=";", quotechar='"', quoting=csv.QUOTE_MINIMAL
      )
      writer.writeheader()

      for row in reader:
         row["old_marker"] = "1" if row.get("url", "").strip() in marked_urls else "0"
         writer.writerow(row)


if __name__ == "__main__":
   old_csv = Path("web_scraper/google_scholar_comp.csv")
   new_csv = Path("web_scraper/papers_regex_filtered_dedup_split.csv")
   out_csv = Path("web_scraper/papers_regex_filtered_dedup_split_marked.csv")

   marked_set = collect_marked_urls(old_csv)
   copy_with_marker(new_csv, out_csv, marked_set)

   print(f"Done.  {out_csv.resolve()} now has an 'old_marker' column (1 = present in old list).")
