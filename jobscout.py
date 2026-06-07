"""
GeoEco JobScout — Multi-source job scraper with geocode caching.
Scrapes greenjobs.de, jobverde.de, and goodjobs.eu for environmental jobs.
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import json
import os
import re
from datetime import datetime
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

# --- CONFIGURATION ---
SEARCH_TERMS = [
    "geoökologie", "umweltwissenschaften", "hydrologie",
    "naturschutz", "klimaschutz"
]

SKILL_KEYWORDS = {
    "GIS/Remote Sensing": ["gis", "arcgis", "qgis", "fernerkundung", "sentinel", "geoinformatik"],
    "Data Science": ["python", "sql", "modellierung", "data", "datenanalyse", "statistik", "r-programm"],
    "Lab/Field": ["probenahme", "boden", "wasser", "labor", "gelände", "kartierung", "field", "feld"],
    "Planning/Law": ["uvp", "genehmigung", "bauleitplanung", "artenschutz", "gutachten", "recht"]
}

MAJOR_CITIES = [
    "Berlin", "Potsdam", "Hamburg", "München", "Köln", "Frankfurt", "Stuttgart",
    "Düsseldorf", "Leipzig", "Dortmund", "Essen", "Bremen", "Dresden", "Hannover",
    "Nürnberg", "Duisburg", "Bochum", "Wuppertal", "Bielefeld", "Bonn", "Münster",
    "Karlsruhe", "Mannheim", "Augsburg", "Wiesbaden", "Gelsenkirchen", "Mönchengladbach",
    "Braunschweig", "Kiel", "Chemnitz", "Aachen", "Halle", "Magdeburg", "Freiburg",
    "Krefeld", "Lübeck", "Mainz", "Erfurt", "Oberhausen", "Rostock", "Kassel",
    "Hagen", "Saarbrücken", "Hamm", "Mülheim", "Ludwigshafen", "Osnabrück", "Oldenburg",
    "Leverkusen", "Solingen", "Darmstadt", "Heidelberg", "Regensburg", "Ingolstadt"
]

# Multi-word cities that the PLZ regex would truncate
COMPOUND_CITIES = [
    "Bad Kreuznach", "Bad Homburg", "Bad Hersfeld", "Bad Nauheim", "Bad Oeynhausen",
    "Bad Salzuflen", "Bad Segeberg", "Bad Vilbel", "Bad Dürkheim",
    "Frankfurt am Main", "Freiburg im Breisgau", "Offenbach am Main",
    "Neustadt an der Weinstraße", "Mülheim an der Ruhr",
    "Schwäbisch Hall", "Schwäbisch Gmünd",
    "Sankt Augustin", "Sankt Ingbert",
]

CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "geocode_cache.json")
JOBS_CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "geoeco_jobs_clean.csv")
CURATED_CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "curated_portfolio.csv")

HEADERS = {"User-Agent": "Mozilla/5.0 (GeoEco-Student-Project/2.0)"}
BERLIN_LAT, BERLIN_LON = 52.5200, 13.4050


# ──────────────────────── GEOCODE CACHE ────────────────────────

class GeocodeCache:
    """Persistent geocode cache backed by a JSON file."""

    def __init__(self, cache_file=CACHE_FILE):
        self.cache_file = cache_file
        self.cache = self._load()

    def _load(self):
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                return {}
        return {}

    def save(self):
        with open(self.cache_file, "w", encoding="utf-8") as f:
            json.dump(self.cache, f, ensure_ascii=False, indent=2)

    def get(self, location):
        return self.cache.get(location)

    def set(self, location, lat, lon):
        self.cache[location] = {"lat": lat, "lon": lon}

    def has(self, location):
        return location in self.cache


# ──────────────────────── LOCATION EXTRACTION ────────────────────────

def extract_location_smart(full_text):
    """Extract city from text using PLZ pattern or known city list."""
    clean_text = full_text.replace("\n", " ").strip()

    # Priority 0: Known compound city names (before PLZ so "Bad Kreuznach" wins over "Bad")
    for city in COMPOUND_CITIES:
        if re.search(r'\b' + re.escape(city) + r'\b', clean_text):
            return city

    # Priority 1: PLZ pattern (e.g. "14473 Potsdam")
    zip_match = re.search(r'\b\d{5}\s+([A-ZÄÖÜ][a-zäöüß]+(?:\s+(?:am|an|im|bei|ob|der|den|dem)\s+[A-ZÄÖÜ][a-zäöüß]+)*)', clean_text)
    if zip_match:
        return zip_match.group(1)

    # Priority 2: Known major cities
    for city in MAJOR_CITIES:
        if re.search(r'\b' + re.escape(city) + r'\b', clean_text):
            return city

    return "Deutschland"


def is_remote_job(text):
    """Check if a job listing indicates remote/home office work."""
    remote_patterns = [
        r'(?i)home\s*office', r'(?i)100\s*%\s*remote',
        r'(?i)\bremote\b', r'(?i)bundesweit', r'(?i)deutschlandweit'
    ]
    return any(re.search(p, text) for p in remote_patterns)


def _extract_work_model(text):
    """Extract work model from text: Remote, Hybrid, or Vor Ort."""
    text_lower = text.lower()
    if re.search(r'100\s*%\s*remote', text_lower):
        return "100% Remote"
    if 'hybrid' in text_lower:
        return "Hybrid"
    if 'nur vor ort' in text_lower or 'vor ort' in text_lower:
        return "Vor Ort"
    if re.search(r'\bremote\b', text_lower):
        return "Remote"
    return ""


# ──────────────────────── SKILLS MATCHER ────────────────────────

def match_skills(text):
    """Match skills from text against keyword categories."""
    text_lower = text.lower()
    found = set()
    for cat, keys in SKILL_KEYWORDS.items():
        if any(k in text_lower for k in keys):
            found.add(cat)
    return ", ".join(sorted(found)) if found else "General"


# ──────────────────────── SCRAPERS ────────────────────────

def scrape_greenjobs(search_term, progress_callback=None):
    """Scrape greenjobs.de for a given search term."""
    if progress_callback:
        progress_callback(f"🔎 Scraping Greenjobs.de: '{search_term}'...")

    url = f"https://www.greenjobs.de/angebote/index.html?z=alle&s={search_term}&loc=&countrycode=de&dist=10&lng=&lat="

    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
    except Exception as e:
        if progress_callback:
            progress_callback(f"❌ Greenjobs error: {e}")
        return []

    soup = BeautifulSoup(response.content, 'html.parser')
    jobs = []

    BLACKLIST = [
        "login.html", "inserieren.html", "neueste.html", "infos.html",
        "agb.html", "datenschutz.html", "newsletteranmeldung.html",
        "newsletter", "anmeldung",
    ]

    TITLE_BLACKLIST = [
        "anmeldung arbeitgeber", "login", "newsletter", "jobsucher",
    ]

    for link in soup.find_all("a", href=True):
        href = link['href']

        if ("/stellenanzeige/" in href or "/angebote/" in href) and not any(x in href for x in BLACKLIST):
            if "index.html" in href:
                continue

            title = link.get_text(strip=True)
            if len(title) < 4:
                continue

            # Filter out newsletter/login page leaks by title
            if any(bl in title.lower() for bl in TITLE_BLACKLIST):
                continue

            full_link = href if href.startswith("http") else f"https://www.greenjobs.de{href}"

            container = link.parent.parent
            full_row_text = container.get_text(" | ", strip=True) if container else title

            location = extract_location_smart(full_row_text)

            # Map remote jobs to Berlin
            if (location == "Deutschland" or location == "Homeoffice") and is_remote_job(full_row_text):
                location = "Berlin (Remote)"

            # Extract company
            company_candidate = full_row_text.replace(title, "").replace(location, "")
            company_candidate = re.sub(r'\d{5}', '', company_candidate)
            company_candidate = re.sub(r'Bewerbungsfrist.*', '', company_candidate)
            company_candidate = company_candidate.replace("|", "").strip()
            company_candidate = re.sub(r'\(Remote\)', '', company_candidate).strip()

            if len(company_candidate) < 3 or "Deutschland" in company_candidate:
                company = "Not specified"
            else:
                company = company_candidate[:80] + "..." if len(company_candidate) > 80 else company_candidate

            work_model = _extract_work_model(full_row_text)

            jobs.append({
                "Title": title,
                "Company": company,
                "Location": location.replace(" (Remote)", ""),
                "Remote": "Berlin (Remote)" in location or is_remote_job(full_row_text),
                "Link": full_link,
                "Source": "Greenjobs",
                "Term": search_term,
                "Salary": "",
                "Employment_Type": "",
                "Posted": "",
                "Work_Model": work_model,
            })

    return jobs


def scrape_jobverde(search_term, progress_callback=None):
    """Scrape jobverde.de for a given search term."""
    if progress_callback:
        progress_callback(f"🔎 Scraping Jobverde.de: '{search_term}'...")

    url = f"https://www.jobverde.de/gruene-jobs/?suche={search_term}&wo=&umkreis=20000&festanstellung=false"

    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
    except Exception as e:
        if progress_callback:
            progress_callback(f"❌ Jobverde error: {e}")
        return []

    soup = BeautifulSoup(response.content, 'html.parser')
    jobs = []
    seen_links = set()

    job_link_pattern = re.compile(
        r'/gruene-jobs/[^?]+/[^?]+-\d+$'
        r'|/stellenanzeigen-special/'
    )

    # Navigation/category links to exclude
    NAV_BLACKLIST = [
        "/gruene-jobs?", "/gruene-jobs/?page=",
        "jobalert", "newsletter", "login", "seminare-events",
        "stellenanzeigen-schalten", "fuer-arbeitgeber", "fuer-bewerber",
        "gruene-arbeitgeber", "karriere-guide", "karrieremessen",
        "gruene-events", "gruene-studiengaenge", "nachhaltige-studiengaenge",
        "gruene-weiterbildungen", "ueber-jobverde", "magazin",
        "kontakt", "impressum", "datenschutz", "agb", "mediadaten",
        "partner", "startseite", "karriereinfos",
        "gruene-jobs-in-",
    ]

    for link in soup.find_all("a", href=True):
        href = link.get("href", "")

        # Build full URL for matching
        full_link = href if href.startswith("http") else f"https://www.jobverde.de/{href.lstrip('/')}"

        # Skip navigation/non-job links
        if any(bl in href.lower() for bl in NAV_BLACKLIST):
            continue

        if not job_link_pattern.search(href):
            continue

        title = link.get_text(strip=True)
        if len(title) < 5:
            continue

        # Filter out "Weiterer Link zur Stellenanzeige" entries
        if "weiterer link" in title.lower():
            continue

        # Skip /stellenanzeigen-special/ duplicate links
        if "/stellenanzeigen-special/" in href:
            continue

        if full_link in seen_links:
            continue
        seen_links.add(full_link)

        company = "See listing"
        if "|" in title:
            parts = title.rsplit("|", 1)
            title = parts[0].strip()
            company = parts[1].strip() if len(parts) > 1 and len(parts[1].strip()) > 2 else "See listing"

        parent = link.parent
        if parent:
            container = parent.parent if parent.parent else parent
            full_text = container.get_text(" | ", strip=True)
        else:
            full_text = title

        location = extract_location_smart(full_text)
        work_model = _extract_work_model(full_text)

        if (location == "Deutschland") and is_remote_job(full_text):
            location = "Berlin"
            remote = True
        else:
            remote = is_remote_job(full_text)

        jobs.append({
            "Title": title,
            "Company": company,
            "Location": location,
            "Remote": remote,
            "Link": full_link,
            "Source": "Jobverde",
            "Term": search_term,
            "Salary": "",
            "Employment_Type": "",
            "Posted": "",
            "Work_Model": work_model,
        })

    return jobs


def scrape_goodjobs(search_term, progress_callback=None):
    """Scrape goodjobs.eu for a given search term.

    GoodJobs renders job cards server-side when using the correct
    search URL (?search=). We parse the structured HTML job cards
    to extract title, company, location, salary, employment type, etc.
    """
    if progress_callback:
        progress_callback(f"🔎 Scraping GoodJobs.eu: '{search_term}'...")

    url = f"https://goodjobs.eu/jobs?search={search_term}&items_per_page=50"

    try:
        response = requests.get(url, headers=HEADERS, timeout=20)
        if response.status_code != 200:
            if progress_callback:
                progress_callback(f"⚠️ GoodJobs returned status {response.status_code}")
            return []
    except Exception as e:
        if progress_callback:
            progress_callback(f"❌ GoodJobs error: {e}")
        return []

    soup = BeautifulSoup(response.content, 'html.parser')
    jobs = []
    seen_links = set()

    # Find all job card anchor elements with class "jobcard"
    job_cards = soup.find_all("a", class_="jobcard")

    for card in job_cards:
        href = card.get("href", "")
        if not href:
            continue

        full_link = href if href.startswith("http") else f"https://goodjobs.eu{href}"

        if full_link in seen_links:
            continue
        seen_links.add(full_link)

        # Extract title from h2 with itemprop="name"
        title_el = card.find("h2", itemprop="name")
        title = title_el.get_text(strip=True) if title_el else ""
        if len(title) < 3:
            continue

        # Extract company name — it's in a span inside a div with
        # class containing "inline-flex" that has a building icon SVG before it
        company = ""
        company_spans = card.find_all("span", class_="leading-none")
        if company_spans:
            # First leading-none span inside the card body is the company name
            company = company_spans[0].get_text(strip=True) if company_spans else ""

        # Extract metadata by scanning ALL divs that contain an SVG icon
        location = ""
        salary = ""
        employment_type = ""
        working_time = ""
        posted = ""
        work_model = ""

        all_divs = card.find_all("div")
        for div in all_divs:
            # Only process divs that directly contain an SVG
            svg = div.find("svg", recursive=False)
            if not svg:
                # Also check for SVG as first-level child
                svg = div.find("svg")
                if not svg:
                    continue

            svg_path = svg.find("path")
            if not svg_path:
                continue

            d_attr = svg_path.get("d", "")

            # Map pin icon (location)
            if "M12 20.8995" in d_attr or "M12 23.7279" in d_attr:
                loc_div = div.find("div", class_="flex")
                if loc_div:
                    loc_text = loc_div.get_text(" ", strip=True)
                    parts = [p.strip() for p in loc_text.split("|")]
                    location = parts[0] if parts else ""
                    if len(parts) > 1:
                        work_model = parts[1].strip()

            # Clock icon (working time)
            elif "M12 22C6.47715 22" in d_attr and "M13 12H17V14H11V7H13V12Z" in d_attr:
                time_div = div.find("div", class_="flex")
                if time_div:
                    working_time = time_div.get_text(strip=True)

            # Euro icon (salary)
            elif "M12.0049 22.0027" in d_attr:
                sal_div = div.find("div", class_="flex")
                if sal_div:
                    salary = sal_div.get_text(strip=True)

            # Briefcase icon (employment type)
            elif "M7 5V2C7" in d_attr and "M9 3V5H15V3H9" in d_attr:
                emp_div = div.find("div", class_="flex")
                if emp_div:
                    employment_type = emp_div.get_text(strip=True)

            # Calendar icon (posted date)
            elif "M9 1V3H15V1H17V3H21" in d_attr:
                # The posted date span may be a sibling of the SVG
                posted_span = div.find("span", class_="leading-none")
                if posted_span:
                    posted = posted_span.get_text(strip=True)

        # Fallback location extraction from full text
        if not location or location == "":
            full_text = card.get_text(" ", strip=True)
            location = extract_location_smart(full_text)

        remote = is_remote_job(work_model) if work_model else is_remote_job(card.get_text(" ", strip=True))

        if (location == "Deutschland" or not location) and remote:
            location = "Berlin"

        jobs.append({
            "Title": title,
            "Company": company if company else "See listing",
            "Location": location,
            "Remote": remote,
            "Link": full_link,
            "Source": "GoodJobs",
            "Term": search_term,
            "Salary": salary,
            "Employment_Type": employment_type,
            "Posted": posted,
            "Work_Model": work_model,
        })

    if progress_callback:
        progress_callback(f"   Found {len(jobs)} GoodJobs listings for '{search_term}'")

    return jobs


# ──────────────────────── GEOCODING ────────────────────────

def geocode_locations(df, cache, progress_callback=None):
    """Geocode unique locations, using cache when available."""
    if progress_callback:
        progress_callback("🌍 Geocoding locations...")

    geolocator = Nominatim(user_agent="geoeco_student_potsdam_v2")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

    unique_locs = [loc for loc in df['Location'].unique()
                   if loc not in ("Deutschland", "Homeoffice", "")]

    new_lookups = 0
    for loc in unique_locs:
        if cache.has(loc):
            continue

        try:
            geo = geocode(f"{loc}, Deutschland")
            if geo:
                cache.set(loc, geo.latitude, geo.longitude)
                new_lookups += 1
                if progress_callback:
                    progress_callback(f"   📍 {loc} → OK")
        except Exception:
            if progress_callback:
                progress_callback(f"   ❌ {loc} → Failed")

    if new_lookups > 0:
        cache.save()
        if progress_callback:
            progress_callback(f"💾 Cached {new_lookups} new locations")

    # Map coordinates
    def get_lat(loc):
        if loc in ("Deutschland", "Homeoffice", ""):
            return None
        cached = cache.get(loc)
        return cached["lat"] if cached else None

    def get_lon(loc):
        if loc in ("Deutschland", "Homeoffice", ""):
            return None
        cached = cache.get(loc)
        return cached["lon"] if cached else None

    df['Lat'] = df['Location'].map(get_lat)
    df['Lon'] = df['Location'].map(get_lon)

    # For remote jobs without coordinates, place at Berlin
    remote_mask = df['Remote'] & df['Lat'].isna()
    df.loc[remote_mask, 'Lat'] = BERLIN_LAT
    df.loc[remote_mask, 'Lon'] = BERLIN_LON
    df.loc[remote_mask, 'Location'] = 'Berlin (Remote)'

    return df


# ──────────────────────── MAIN ORCHESTRATOR ────────────────────────

def run_full_scrape(search_terms=None, sources=None, progress_callback=None):
    """
    Run a full scrape across all configured sources and search terms.
    Returns a cleaned DataFrame with geocoded locations.
    """
    if search_terms is None:
        search_terms = SEARCH_TERMS
    if sources is None:
        sources = ["greenjobs", "jobverde", "goodjobs"]

    scraper_map = {
        "greenjobs": scrape_greenjobs,
        "jobverde": scrape_jobverde,
        "goodjobs": scrape_goodjobs,
    }

    all_jobs = []
    total_steps = len(search_terms) * len(sources)
    step = 0

    for term in search_terms:
        for source_name in sources:
            scraper = scraper_map.get(source_name)
            if scraper:
                jobs = scraper(term, progress_callback)
                all_jobs.extend(jobs)
                step += 1
                if progress_callback:
                    progress_callback(f"📊 Progress: {step}/{total_steps} ({len(all_jobs)} jobs so far)")
            time.sleep(0.5)  # Be polite between requests

    if not all_jobs:
        if progress_callback:
            progress_callback("❌ No jobs found.")
        return pd.DataFrame()

    df = pd.DataFrame(all_jobs)

    # Merge duplicate links (combine terms)
    df['Term'] = df.groupby('Link')['Term'].transform(lambda x: ', '.join(x.unique()))
    df = df.drop_duplicates(subset=['Link'])

    # Add skills
    df['Skills'] = df.apply(
        lambda r: match_skills(f"{r['Title']} {r['Term']} {r.get('Company', '')}"),
        axis=1
    )

    # Geocode
    cache = GeocodeCache()
    df = geocode_locations(df, cache, progress_callback)

    # Reorder columns for cleaner output
    col_order = [
        'Title', 'Company', 'Location', 'Remote', 'Link', 'Source', 'Term',
        'Skills', 'Salary', 'Employment_Type', 'Posted', 'Work_Model',
        'Lat', 'Lon'
    ]
    for col in col_order:
        if col not in df.columns:
            df[col] = ""
    df = df[col_order]

    # Save
    df.to_csv(JOBS_CSV, index=False)
    if progress_callback:
        progress_callback(f"✅ Done! {len(df)} unique jobs found and saved.")

    return df


def load_existing_jobs():
    """Load previously scraped jobs from CSV."""
    if os.path.exists(JOBS_CSV):
        try:
            return pd.read_csv(JOBS_CSV)
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()


def ensure_curated_db():
    """Ensure the curated portfolio CSV exists."""
    if not os.path.exists(CURATED_CSV):
        cols = ['Company', 'Job_Type_or_Title', 'Location', 'Source', 'Link', 'Notes', 'Status']
        pd.DataFrame(columns=cols).to_csv(CURATED_CSV, index=False)


def seed_geocode_cache_from_csv():
    """Pre-seed the geocode cache from existing CSV data."""
    cache = GeocodeCache()
    if os.path.exists(JOBS_CSV):
        df = pd.read_csv(JOBS_CSV)
        if 'Location' in df.columns and 'Lat' in df.columns and 'Lon' in df.columns:
            for _, row in df.iterrows():
                loc = str(row.get('Location', ''))
                lat = row.get('Lat')
                lon = row.get('Lon')
                if loc and loc not in ('Deutschland', 'Homeoffice', '', 'nan') and pd.notna(lat) and pd.notna(lon):
                    if not cache.has(loc):
                        cache.set(loc, float(lat), float(lon))
            cache.save()
            print(f"Seeded cache with {len(cache.cache)} locations")


# --- CLI EXECUTION ---
if __name__ == "__main__":
    # Seed cache from existing data first
    seed_geocode_cache_from_csv()

    def print_progress(msg):
        print(msg)

    df = run_full_scrape(progress_callback=print_progress)
    if not df.empty:
        print(f"\nResults by source:")
        if 'Source' in df.columns:
            print(df['Source'].value_counts().to_string())