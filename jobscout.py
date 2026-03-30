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

    # Priority 1: PLZ pattern (e.g. "14473 Potsdam")
    zip_match = re.search(r'\b\d{5}\s+([A-ZÄÖÜ][a-zäöüß]+)', clean_text)
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
        r'(?i)home\s*office', r'(?i)remote', r'(?i)100\s*%\s*remote',
        r'(?i)bundesweit', r'(?i)deutschlandweit'
    ]
    return any(re.search(p, text) for p in remote_patterns)


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

    BLACKLIST = ["login.html", "inserieren.html", "neueste.html", "infos.html", "agb.html", "datenschutz.html"]

    for link in soup.find_all("a", href=True):
        href = link['href']

        if ("/stellenanzeige/" in href or "/angebote/" in href) and not any(x in href for x in BLACKLIST):
            if "index.html" in href:
                continue

            title = link.get_text(strip=True)
            if len(title) < 4:
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

            jobs.append({
                "Title": title,
                "Company": company,
                "Location": location.replace(" (Remote)", ""),
                "Remote": "Berlin (Remote)" in location or is_remote_job(full_row_text),
                "Link": full_link,
                "Source": "Greenjobs",
                "Term": search_term
            })

    return jobs


def scrape_jobverde(search_term, progress_callback=None):
    """Scrape jobverde.de for a given search term."""
    if progress_callback:
        progress_callback(f"🔎 Scraping Jobverde.de: '{search_term}'...")

    url = f"https://www.jobverde.de/stellenanzeigen/alle/{search_term}"

    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
    except Exception as e:
        if progress_callback:
            progress_callback(f"❌ Jobverde error: {e}")
        return []

    soup = BeautifulSoup(response.content, 'html.parser')
    jobs = []

    # Look for job listing cards/links
    job_links = soup.find_all("a", href=True)
    seen_links = set()

    for link in job_links:
        href = link.get("href", "")
        # Jobverde job listings follow pattern /stellenanzeigen/xxxxx/title
        if "/stellenanzeigen/" not in href:
            continue
        if "/alle/" in href or "/gruene-" in href:
            continue

        title = link.get_text(strip=True)
        if len(title) < 5:
            continue

        full_link = href if href.startswith("http") else f"https://www.jobverde.de{href}"

        if full_link in seen_links:
            continue
        seen_links.add(full_link)

        # Try to get context from parent
        parent = link.parent
        full_text = parent.get_text(" | ", strip=True) if parent else title
        location = extract_location_smart(full_text)

        if (location == "Deutschland") and is_remote_job(full_text):
            location = "Berlin"
            remote = True
        else:
            remote = is_remote_job(full_text)

        jobs.append({
            "Title": title,
            "Company": "See listing",
            "Location": location,
            "Remote": remote,
            "Link": full_link,
            "Source": "Jobverde",
            "Term": search_term
        })

    return jobs


def scrape_goodjobs(search_term, progress_callback=None):
    """Scrape goodjobs.eu for a given search term."""
    if progress_callback:
        progress_callback(f"🔎 Scraping GoodJobs.eu: '{search_term}'...")

    url = f"https://goodjobs.eu/de/jobs?search_term={search_term}&location=Deutschland"

    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
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

    # GoodJobs uses various card-like structures
    for link in soup.find_all("a", href=True):
        href = link.get("href", "")
        if "/de/jobs/" not in href and "/job/" not in href:
            continue

        title = link.get_text(strip=True)
        if len(title) < 5:
            continue

        full_link = href if href.startswith("http") else f"https://goodjobs.eu{href}"

        if full_link in seen_links:
            continue
        seen_links.add(full_link)

        parent = link.parent
        full_text = parent.get_text(" | ", strip=True) if parent else title
        location = extract_location_smart(full_text)

        if (location == "Deutschland") and is_remote_job(full_text):
            location = "Berlin"

        jobs.append({
            "Title": title,
            "Company": "See listing",
            "Location": location,
            "Remote": is_remote_job(full_text),
            "Link": full_link,
            "Source": "GoodJobs",
            "Term": search_term
        })

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