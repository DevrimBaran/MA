from __future__ import annotations
import csv
import sys
from pathlib import Path
from typing import List

csv.field_size_limit(sys.maxsize)

# excel/libreoffice has a limit of 32,767 characters per row
CHUNK = 30_000
ABSTR_COL = "full_abstract"

def chunk_text(text: str, size: int) -> List[str]:
   "Split text into pieces of length <= size"
   return [text[i : i + size] for i in range(0, len(text), size)] or [""]

def split_abstracts(src: Path, dest: Path | None = None) -> Path:
   if dest is None:
      dest = src.with_name(src.stem + "_split" + src.suffix)

   # Pass 1: read, split, remember how many parts every row needs
   rows: List[dict] = []
   max_parts = 1

   with src.open(newline="", encoding="utf-8") as f:
      reader = csv.DictReader(f, delimiter=";", quotechar='"')
      if ABSTR_COL not in reader.fieldnames:
         sys.exit(f"Error: '{ABSTR_COL}' column not found in {src}")

      for row in reader:
         parts = chunk_text(row.get(ABSTR_COL, ""), CHUNK)
         max_parts = max(max_parts, len(parts))
         row[ABSTR_COL] = parts            # temporarily store the list
         rows.append(row)

   # Build full header: keep all originals except ABSTR_COL, then add
   # full_abstract_p1 ... pN (N = max_parts)
   base_fields = [h for h in reader.fieldnames if h != ABSTR_COL]
   abstr_fields = [f"{ABSTR_COL}_p{i}" for i in range(1, max_parts + 1)]
   fieldnames = base_fields + abstr_fields

   # Pass 2: write out with the expanded columns
   with dest.open("w", newline="", encoding="utf-8") as f:
      writer = csv.DictWriter(
         f, fieldnames=fieldnames,
         delimiter=";", quotechar='"', quoting=csv.QUOTE_MINIMAL
      )
      writer.writeheader()

      for row in rows:
         parts: List[str] = row.pop(ABSTR_COL)           # retrieve list
         # Fill missing chunks with empty strings so every row matches header
         parts += [""] * (max_parts - len(parts))
         for i, part in enumerate(parts, start=1):
               row[f"{ABSTR_COL}_p{i}"] = part
         writer.writerow(row)

   return dest


if __name__ == "__main__":
   input_path = Path("web_scraper/papers_regex_filtered.csv")
   output_path = Path("web_scraper/papers_regex_filtered_split.csv")

   out_file = split_abstracts(input_path, output_path)
   print(f"Done. Split-friendly file written to {out_file.resolve()}")