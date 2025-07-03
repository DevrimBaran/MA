import csv
import os
import tempfile
import shutil
import pathlib
import time
import re
import signal
import atexit
import sys
import tempfile
import shutil
import requests
import PyPDF2
import warnings
from urllib3.exceptions import InsecureRequestWarning
from urllib.parse import quote_plus as qp

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, InvalidSessionIdException, WebDriverException
from PyPDF2.errors import PdfReadError
from PyPDF2 import PdfReader

start = time.time()

# Define synonyms and queries
SYNONYMS = {
    "spsc": ["single-producer single-consumer", "single-writer single-reader", "one-to-one"],
    "spmc": ["single-producer multi-consumer", "single-writer multi-reader", "one-to-many"],
    "mpsc": ["multi-producer single-consumer", "multi-writer single-reader", "many-to-one"],
    "mpmc": ["multi-producer multi-consumer", "multi-writer multi-reader", "many-to-many"],
}
QUERIES = ["wait-free queue"]
for acr, phrases in SYNONYMS.items():
    items = [f'"{acr}"'] + [f'"{p}"' for p in phrases]
    clause = ' OR '.join(items)
    QUERIES.append(f'"wait-free" ({clause}) "queue"')

# Google Scholar configuration
def url_pattern(q):
    return f"https://scholar.google.com/scholar?q={qp(q)}&hl=en&as_sdt=0,5"
GS_CFG = {
    "url_pattern": url_pattern,
    "title_css": "h3.gs_rt a",
    "year_css": "div.gs_a",
    "abs_css": "div.gs_rs",
    "next_locator": {"by": By.CSS_SELECTOR, "value": ".gs_ico_nav_next"},
    "output": "google_scholar_full_abstracts_spsc.csv"
}

# Common abstract selectors
ABSTRACT_SELECTORS = [
    {"domain": "dl.acm.org", "selectors": [
        "div.abstractSection p", "div.abstract-text", ".abstract-field p", "#abstract-content", "div[id*='abstract']"
    ]},
    {"domain": "ieeexplore.ieee.org", "selectors": [
        "div.abstract-text", "div.article-abstract", "meta[name='description']", "div.abstract-container p", "div.abstract-wrapper p"
    ]},
    {"domain": "sciencedirect.com", "selectors": [
        "div.abstract.author"
    ]},
    {"domain": "springer.com", "selectors": [
        "div#Abs1-content p", "section.Abstract p", "div.c-article-section__content p", "p.Para"
    ]},
    {"domain": "arxiv.org", "selectors": [
        "blockquote.abstract", "span.abstract-full p", "div.abstract"
    ]},
    {"domain": "researchgate.net", "selectors": [
        "div.research-detail-header-section__abstract",
        "div.nova-legacy-c-card__body div.nova-legacy-e-text",
        "div.publication-abstract"
    ]},
    {"domain": "onlinelibrary.wiley.com", "selectors": [
        "div.article-section__content p", "section.article-section__abstract p", "div.article-abstract p", "div.abstract p", "div.en-abstract", "section[aria-labelledby*='abstract'] p"
    ]},
    {"domain": "mdpi.com", "selectors": [
        "div.html-abstract", "div.art-abstract div", "section.html-abstract"
    ]},
    {"domain": "tandfonline.com", "selectors": [
        "div.abstractSection p", "div[class*='NLM_abstract'] p", "meta[name='dc.Description']", "div.abstract-group p"
    ]},
    {"domain": "journals.sagepub.com", "selectors": [
        "div[class*='abstractSection'] p", "div.abstract div.abstractInFull", "div#abstract-content", "meta[name='dc.Description']"
    ]},
    {"domain": "jstor.org", "selectors": [
        "div.abstract p", "div.mtl p", "div#abstract", "div[class*='abstract']"
    ]},
    {"domain": "academic.oup.com", "selectors": [
        "section.abstract p", "div.abstract-content", "section[aria-labelledby*='abstract']"
    ]},
    {"domain": "ncbi.nlm.nih.gov", "selectors": [
        "div#abstract-content", "div.abstract-content p", "div.AbstractText", "abstract"
    ]},
    {"domain": "citeseerx.ist.psu.edu", "selectors": [
        "div.abstract", "div#abstract", "div[class*='abstract']"
    ]},
    {"domain": "", "selectors": [
        "meta[name='description']", "meta[property='og:description']", "meta[name='citation_abstract']",
        "div[id*='abstract']", "section[id*='abstract']", "div[class*='abstract']", "section[class*='abstract']",
        "p.abstract", "div.abstract p", "#abstract", ".abstract", "div.summary p", ".paper-abstract",
        "[aria-labelledby*='abstract']", "[id*='abstract']", "article-text.article-section-abstract", "abstract"
    ]},
]

def is_pdf_url(url):
    """Quickly tell if this URL serves a PDF by HEAD’ing it."""
    url_lower = url.lower()
    if "citeseerx.ist.psu.edu" in url_lower and "type=pdf" in url_lower:
        print(f"      Identified CiteSeerX PDF URL: {url}")
        return True
        
    # Check other common URL patterns
    if url_lower.endswith(".pdf") or "type=pdf" in url_lower or "/pdf/" in url_lower:
        print(f"      Identified PDF URL by pattern: {url}")
        return True
        
    # Try HEAD request approach
    try:
        head = requests.head(url, allow_redirects=True, timeout=10)
        ctype = head.headers.get("Content-Type", "").lower()
        if "pdf" in ctype:
            print(f"      Identified PDF URL by Content-Type: {url}")
            return True
    except Exception as e:
        print(f"      HEAD request failed: {e}")
        # Continue to the next check
    
    # As a last resort, check for PDF magic bytes
    try:
        headers = {'Range': 'bytes=0-5'}
        resp = requests.get(url, headers=headers, stream=True, timeout=10, allow_redirects=True)
        for chunk in resp.iter_content(chunk_size=5):
            if chunk and chunk.startswith(b'%PDF-'):
                print(f"      Identified PDF URL by magic bytes: {url}")
                return True
            break  # Only need to check first chunk
    except Exception as e:
        print(f"      Magic bytes check failed: {e}")
    
    return False

def download_and_parse_pdf(url):
    """
    Download a PDF from URL, concatenate all pages, and extract an Abstract section.
    If no explicit Abstract header is found, return the first 500 chars of the full text.
    """
    """
    Download a PDF from URL, concatenate all pages, and extract an Abstract section.
    If no explicit Abstract header is found, return the first 500 chars of the full text.
    """
    try:
        resp = requests.get(url, stream=True, timeout=60, verify=False)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"      PDF download failed ({e.__class__.__name__}): {url}")
        return "ABSTRACT_NOT_FOUND"
    fd, path = tempfile.mkstemp(suffix=".pdf")
    os.close(fd)
    try:
        with open(path, "wb") as f:
            for chunk in resp.iter_content(1024*16):
                if not chunk:
                    break
                f.write(chunk)
                
        # helper to load reader
        def make_reader():
            return PyPDF2.PdfReader(path, strict=False)

        # first attempt
        try:
            reader = make_reader()

        # catch malformed-EOF/startxref
        except PdfReadError as e:
            msg = str(e)
            if "EOF marker not found" in msg:
                # append EOF only
                with open(path, "ab") as f:
                    f.write(b"\n%%EOF\n")
                reader = make_reader()

            elif "startxref not found" in msg:
                # append minimal xref/trailer/startxref
                trailer = (
                    b"\nxref\n0 1\n"
                    b"0000000000 65535 f \n"
                    b"trailer\n<< /Size 1 >>\n"
                    b"startxref\n0\n"
                    b"%%EOF\n"
                )
                with open(path, "ab") as f:
                    f.write(trailer)
                reader = make_reader()

            else:
                print(f"      PDF read error: {e}")
                return "ABSTRACT_NOT_FOUND"
        except Exception as e:
            print(f"      PDF read error: {e}")
            return "ABSTRACT_NOT_FOUND"

        # Build full text from all pages
        pages = [p.extract_text() or "" for p in reader.pages]
        full_text = "\n".join(pages)
        
        first_page = reader.pages[0].extract_text() or ""
        first_pages = min(5, len(reader.pages))
        pages_text = [reader.pages[i].extract_text() or "" for i in range(first_pages)]
        first_text = "\n".join(pages_text)
        m1 = re.search(
            r'(?i)abstract[:.\s]*\r?\n+'
            r'([\s\S]+?)'
            r'(?:\r?\n\s*\r?\n|1\.\s*Introduction|keywords?:)'
            , first_page
        )
        if m1:
            return re.sub(r'\s+', ' ', m1.group(1).strip())
        
        # Try to locate the Abstract header
        m = re.search(r'(?:Abstract|ABSTRACT)[:\s\n]*', full_text)
        if m:
            start = m.end()
            snippet = full_text[start:start + 5000]

            # Stop at the next section heading (e.g. "1. Introduction", "Keywords", etc.)
            heading_re = re.compile(r'\n\s*(?:\d+\.\s*)?[A-Z][A-Za-z0-9 ]{1,50}\s*\n')
            hm = heading_re.search(snippet)
            if hm:
                snippet = snippet[:hm.start()]

            # Remove only newlines
            return snippet.replace('\r', ' ').replace('\n', ' ').strip()
        
                # First try a pattern that looks for "Abstract" followed by text until a section break
        m1 = re.search(
            r'(?i)abstract[:.\s]*\r?\n+'  # "Abstract:" followed by line breaks
            r'([\s\S]+?)'                  # Capture all text
            r'(?:\r?\n\s*\r?\n|1\.\s*Introduction|keywords?:|categories|index terms)', # Until next section
            first_text
        )
        if m1:
            return re.sub(r'\s+', ' ', m1.group(1).strip())
        
        # Try more generic pattern
        m2 = re.search(r'(?i)abstract[:.\s]*\n([\s\S]+?)(?:\n\s*\n|\d\.\s+\w+)', first_text)
        if m2:
            return re.sub(r'\s+', ' ', m2.group(1).strip())
            
        # Another attempt - just find "Abstract" and take some text after it
        m3 = re.search(r'(?i)abstract[:\s]*([\s\S]{20,1500})', first_text)
        if m3:
            text = m3.group(1).strip()
            # Try to limit it to just the abstract by stopping at common section markers
            end_markers = ["\n\n", "\n1.", "\nKeywords", "\nCategories", "\nIndex Terms"]
            for marker in end_markers:
                pos = text.find(marker)
                if pos > 100:  # Make sure we have enough abstract text
                    text = text[:pos]
                    break
            return re.sub(r'\s+', ' ', text)

        # Fallback: no Abstract header -> return entire text, stripped of newlines
        return full_text.replace('\r', ' ').replace('\n', ' ').strip()

    except Exception as e:
        print(f"      PDF parse error: {e}")
        return "ABSTRACT_NOT_FOUND"
    finally:
        try:
            os.remove(path)
        except Exception as e:
            print(f"      Error removing temp file: {e}")
            pass

# Settings
def random_pause(seconds):
    time.sleep(seconds)

PAGE_TIMEOUT = 30
ELEMENT_TIMEOUT = 15
MAX_ARTICLES_PER_QUERY = float('inf')
HEADLESS = False

def open_chrome():
    profile_dir = tempfile.mkdtemp(prefix="scholar_profile_")
    options = uc.ChromeOptions()
    if HEADLESS:
        options.add_argument('--headless=new')
    options.add_argument(f"--user-data-dir={profile_dir}")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--lang=en-US,en;q=0.9")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    print(f"Starting browser with fresh profile at {profile_dir}...")
    driver = uc.Chrome(options=options, use_subprocess=True)
    driver.set_page_load_timeout(PAGE_TIMEOUT)
    driver.set_script_timeout(PAGE_TIMEOUT)
    
    # register normal cleanup (quit+rmtree) on process exit
    def _cleanup():
        try:
            driver.quit()
        except:
            pass
        shutil.rmtree(profile_dir, ignore_errors=True)
    atexit.register(_cleanup)
    
    # register cleanup on SIGINT / SIGTERM
    def _signal_handler(signum, frame):
        # restore default handler so a second Ctrl-C will actually kill us
        signal.signal(signum, signal.SIG_DFL)
        _cleanup()
        sys.exit(1)
    signal.signal(signal.SIGINT,  _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)
    
    return driver

def extract_full_abstract(driver, url):
    if "dial.uclouvain.be/downloader" in url.lower():
        return "ABSTRACT_NOT_FOUND"
    # Handle PDFs
    if is_pdf_url(url):
        print(f"      Downloading and parsing PDF: {url}")
        abstract = download_and_parse_pdf(url)
        return abstract
    # HTML
    try:
        print(f"      Accessing {url}")
        try:
            driver.get(url)
        except (WebDriverException, TimeoutException):
            return "ABSTRACT_NOT_FOUND"
        random_pause(1)
        # Look for any <iframe> whose src looks like a PDF
        for tag in driver.find_elements(By.CSS_SELECTOR, "embed, object"):
            pdf_src = tag.get_attribute("src") or tag.get_attribute("data") or ""
            if is_pdf_url(pdf_src):
                print(f"      Found embedded PDF in {tag.tag_name}: {pdf_src}")
                return download_and_parse_pdf(pdf_src)
        current = driver.current_url
        selectors = []
        for cfg in ABSTRACT_SELECTORS:
            if cfg["domain"] and cfg["domain"] in current:
                selectors += cfg["selectors"]
        for cfg in ABSTRACT_SELECTORS:
            if not cfg["domain"]:
                selectors += cfg["selectors"]
        for sel in selectors:
            try:
                if sel.startswith("meta"):
                    elems = driver.find_elements(By.CSS_SELECTOR, sel)
                    if elems:
                        c = elems[0].get_attribute("content")
                        if c and len(c) > 50:
                            return c.strip()
                else:
                    elems = driver.find_elements(By.CSS_SELECTOR, sel)
                    if elems:
                        t = " ".join(e.text.strip() for e in elems if e.text.strip())
                        if len(t) > 50:
                            return t
            except:
                continue
        if "dl.acm.org" in current:
            try:
                btns = driver.find_elements(By.XPATH, "//a[contains(text(),'View Abstract') or contains(text(),'Show Abstract')]")
                if btns and btns[0].is_displayed():
                    btns[0].click(); 
                random_pause(1)
                for acm_sel in ["div.abstractSection", "#abstract-content", ".abstract-text"]:
                    elems = driver.find_elements(By.CSS_SELECTOR, acm_sel)
                    if elems:
                        t = " ".join(e.text.strip() for e in elems if e.text.strip())
                        if len(t) > 50:
                            return t
            except:
                pass
        if "wiley" in current:
            try:
                for c in driver.find_elements(By.XPATH, "//button[contains(text(),'Accept')]"):
                    if c.is_displayed(): c.click(); break
                for tab in driver.find_elements(By.XPATH, "//a[contains(text(),'Abstract') or //button[contains(text(),'Abstract')]]"):
                    if tab.is_displayed(): tab.click(); break
                for iframe in driver.find_elements(By.TAG_NAME, 'iframe'):
                    try:
                        driver.switch_to.frame(iframe)
                        elems = driver.find_elements(By.CSS_SELECTOR, "div.abstractSection")
                        if elems:
                            t = " ".join(e.text.strip() for e in elems if e.text.strip())
                            if len(t) > 50:
                                driver.switch_to.default_content(); return t
                        driver.switch_to.default_content()
                    except:
                        driver.switch_to.default_content()
            except:
                pass
        body = driver.find_element(By.TAG_NAME, "body").text
        m = re.search(r'(?:Abstract|ABSTRACT)[:\s\n]+([\s\S]{50,500})', body)
        if m:
            return m.group(1).split("\n")[0].strip()
        return "ABSTRACT_NOT_FOUND"
    except Exception as e:
        print(f"      Error: {e}")
        return "ABSTRACT_NOT_FOUND"

def is_driver_active(driver):
    try:
        _ = driver.current_url
        return True
    except (InvalidSessionIdException, WebDriverException):
        return False

def process_article(driver, container, cfg, rank, query):
    title = link = ''
    if container:
        elem = container.find_element(By.CSS_SELECTOR, cfg['title_css'])
        title = elem.text.strip(); 
        link = elem.get_attribute('href')
    if link.lower().endswith(('.ps.gz', '.ps', '.gz')):
        full_abstract = "ABSTRACT_NOT_FOUND"
    else:
        full_abstract = ""
    print(f"    Processing [{rank}]: {title[:40]}...")
    year=authors=venue=citations=snippet=''
    try:
        meta = container.find_element(By.CSS_SELECTOR, cfg['year_css']).text
        authors = re.split(r',|\.{3}|\u2026', meta)[0].strip()
        ym = re.search(r'\b(\d{4})\b', meta); year = ym.group(1) if ym else ''
        vm = re.search(r'\d{4}[\s\-–]*([^\-–]+)', meta); venue = vm.group(1).strip() if vm else ''
    except:
        pass
    try:
        for c in container.find_elements(By.CSS_SELECTOR, '.gs_fl a'):
            m = re.search(r'Cited by (\d+)', c.text)
            if m: citations = m.group(1); break
    except:
        pass
    try:
        snippet = container.find_element(By.CSS_SELECTOR, cfg['abs_css']).text.strip()
    except:
        pass
    if full_abstract != "ABSTRACT_NOT_FOUND":
        if link and is_pdf_url(link):
            print(f"      [PDF] fetching via requests: {link}")
            full_abstract = download_and_parse_pdf(link)
        else:
            if link:
                orig = driver.current_window_handle
                try:               
                    # open new tab with paper
                    driver.execute_script("window.open(arguments[0]);", link)
                    WebDriverWait(driver, ELEMENT_TIMEOUT).until(lambda d: len(d.window_handles) > 1)
                    new_handle = [h for h in driver.window_handles if h != orig][0]
                    driver.switch_to.window(new_handle)
                    try:
                        full_abstract = extract_full_abstract(driver, link)
                    except Exception as e:
                        print(f"      Error extracting abstract: {e}")
                        full_abstract = f"ERROR: {e}"
                except (WebDriverException, TimeoutException):
                    # any problem opening or switching -> no abstract
                    full_abstract = "ABSTRACT_NOT_FOUND"
                finally:
                    # close and switch back
                    try:
                        driver.close()
                        driver.switch_to.window(orig)
                    except Exception:
                        pass
    return [query, rank, title, link, year, authors, venue, citations, snippet, full_abstract, '']

def scrape_google_scholar(driver, cfg, queries):
    print("Scraping Google Scholar")
    output_file = pathlib.Path(cfg['output'])
    with output_file.open('w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow(['query','rank','title','url','year','authors','venue','citations','snippet','full_abstract','alt_urls'])
        for idx, query in enumerate(queries, start=1):
            print(f"   {query} ({idx}/{len(queries)})")
            driver.get(cfg['url_pattern'](query))
            try:
                WebDriverWait(driver, ELEMENT_TIMEOUT).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, cfg['title_css']))
                )
            except TimeoutException:
                print(f" No results for {query}")
                continue

            page = 1
            rank = 1
            more_pages = True
            while more_pages:
                elems = driver.find_elements(By.CSS_SELECTOR, cfg['title_css'])
                if not elems:
                    break
                # Process every article on this page
                for elem in elems:
                    try:
                        cont = elem.find_element(By.XPATH, 'ancestor::div[contains(@class,"gs_r")]')
                    except:
                        cont = None
                    row = process_article(driver, cont, cfg, rank, query)
                    writer.writerow(row)
                    f.flush()
                    rank += 1
                # Attempt to click the "Next" button for the next page
                try:
                    locator = cfg['next_locator']
                    next_btn = driver.find_element(locator['by'], locator['value'])
                    if not next_btn.is_enabled():
                        more_pages = False
                        print(f"    No more pages for {query}")
                        break
                    page += 1
                    print(f"  navigating to page {page}")
                    next_btn.click()
                    WebDriverWait(driver, ELEMENT_TIMEOUT).until(
                        EC.staleness_of(elems[0])
                    )
                except Exception as e:
                    print(f"    no next page")
                    break
    print("Done scraping")

def main():
    driver = None
    # Suppress SSL warnings for pdf download. Okay for shortly downloading PDFs.
    warnings.filterwarnings('ignore', category=InsecureRequestWarning)
    try:
        print("Starting scraper")
        driver = open_chrome()
        print("Visiting Google Scholar")
        driver.get("https://scholar.google.com")
        random_pause(1)
        scrape_google_scholar(driver, GS_CFG, QUERIES)
    except KeyboardInterrupt:
        print("Interrupted by user")
    except Exception as e:
        print(f"Error during scraping: {e}")
    finally:
        print("Browser closed and profile removed")
        end = time.time()
        print(f"Total time: {end - start:.2f} seconds")
        pass

if __name__ == '__main__':
    main()
