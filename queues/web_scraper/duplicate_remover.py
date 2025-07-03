import csv
import pathlib
import sys

csv.field_size_limit(sys.maxsize)

def remove_duplicates(csv_path):
   """
   Remove duplicate rows in the CSV based on the 'url' field.
   """
   
   dest = csv_path.with_name(csv_path.stem + "_dedup" + csv_path.suffix)
   seen = set()
   with csv_path.open('r', newline='', encoding='utf-8') as infile, \
      dest.open('w', newline='', encoding='utf-8') as outfile:
      reader = csv.DictReader(infile, delimiter=';')
      writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames, delimiter=';')
      writer.writeheader()
      for row in reader:
         url = row.get('url', '').strip()
         if url and url not in seen:
               seen.add(url)
               writer.writerow(row)
   
def main():
   remove_duplicates(pathlib.Path('web_scraper/papers_regex_filtered.csv'))
   print("Done removing duplicates.")


if __name__ == '__main__':
   main()