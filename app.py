"""
GeoEco JobScout — Flask Web Application
Serves the single-page job scout app with REST API.
"""

from flask import Flask, render_template, jsonify, request
import pandas as pd
import os
import json
import threading
from datetime import datetime

from jobscout import (
    run_full_scrape, load_existing_jobs, ensure_curated_db,
    seed_geocode_cache_from_csv, SEARCH_TERMS, CURATED_CSV, JOBS_CSV
)

app = Flask(__name__)

# Scraping state
scrape_state = {
    "running": False,
    "messages": [],
    "progress": 0,
    "total": 0,
}


# ──────────────────────── ROUTES ────────────────────────

@app.route("/")
def index():
    """Serve the main single-page app."""
    return render_template("index.html")


# ──────────────────────── API: JOBS ────────────────────────

@app.route("/api/jobs")
def get_jobs():
    """Return all scraped jobs as JSON."""
    df = load_existing_jobs()
    if df.empty:
        return jsonify({"jobs": [], "count": 0})

    # Clean NaN values
    df = df.fillna("")
    jobs = df.to_dict(orient="records")
    return jsonify({"jobs": jobs, "count": len(jobs)})


@app.route("/api/stats")
def get_stats():
    """Return summary statistics."""
    df = load_existing_jobs()
    if df.empty:
        return jsonify({"total": 0, "by_source": {}, "by_skill": {}, "by_location": {}, "by_term": {}})

    stats = {
        "total": len(df),
        "by_source": df['Source'].value_counts().to_dict() if 'Source' in df.columns else {},
        "by_skill": {},
        "by_location": df['Location'].value_counts().head(20).to_dict() if 'Location' in df.columns else {},
        "by_term": {},
    }

    # Count skills (they can be comma-separated)
    if 'Skills' in df.columns:
        skill_counts = {}
        for skills_str in df['Skills'].dropna():
            for skill in str(skills_str).split(", "):
                skill = skill.strip()
                if skill:
                    skill_counts[skill] = skill_counts.get(skill, 0) + 1
        stats["by_skill"] = skill_counts

    # Count terms (they can be comma-separated)
    if 'Term' in df.columns:
        term_counts = {}
        for terms_str in df['Term'].dropna():
            for term in str(terms_str).split(", "):
                term = term.strip()
                if term:
                    term_counts[term] = term_counts.get(term, 0) + 1
        stats["by_term"] = term_counts

    return jsonify(stats)


# ──────────────────────── API: SCRAPE ────────────────────────

@app.route("/api/scrape", methods=["POST"])
def start_scrape():
    """Trigger a new scrape."""
    if scrape_state["running"]:
        return jsonify({"error": "Scrape already in progress"}), 409

    data = request.json or {}
    terms = data.get("terms", SEARCH_TERMS)
    sources = data.get("sources", ["greenjobs", "jobverde", "goodjobs"])

    scrape_state["running"] = True
    scrape_state["messages"] = []
    scrape_state["progress"] = 0
    scrape_state["total"] = len(terms) * len(sources)

    def run():
        def progress(msg):
            scrape_state["messages"].append(msg)
            if "Progress:" in msg:
                try:
                    parts = msg.split("Progress: ")[1].split("/")
                    scrape_state["progress"] = int(parts[0])
                except (IndexError, ValueError):
                    pass

        try:
            run_full_scrape(
                search_terms=terms,
                sources=sources,
                progress_callback=progress
            )
        except Exception as e:
            scrape_state["messages"].append(f"❌ Error: {str(e)}")
        finally:
            scrape_state["running"] = False

    thread = threading.Thread(target=run, daemon=True)
    thread.start()

    return jsonify({"status": "started", "total_steps": scrape_state["total"]})


@app.route("/api/scrape/status")
def scrape_status():
    """Get the current scrape status."""
    return jsonify({
        "running": scrape_state["running"],
        "messages": scrape_state["messages"][-20:],  # Last 20 messages
        "progress": scrape_state["progress"],
        "total": scrape_state["total"],
    })


# ──────────────────────── API: CURATED PORTFOLIO ────────────────────────

@app.route("/api/curated")
def get_curated():
    """Return curated portfolio."""
    ensure_curated_db()
    try:
        df = pd.read_csv(CURATED_CSV)
        df = df.fillna("")
        return jsonify({"jobs": df.to_dict(orient="records"), "count": len(df)})
    except Exception:
        return jsonify({"jobs": [], "count": 0})


@app.route("/api/curated", methods=["POST"])
def add_curated():
    """Add a job to the curated portfolio."""
    ensure_curated_db()
    data = request.json
    if not data:
        return jsonify({"error": "No data provided"}), 400

    new_entry = pd.DataFrame([{
        'Company': data.get('company', ''),
        'Job_Type_or_Title': data.get('title', ''),
        'Location': data.get('location', ''),
        'Source': data.get('source', 'Manual'),
        'Link': data.get('link', ''),
        'Notes': data.get('notes', ''),
        'Status': data.get('status', 'Watchlist'),
    }])

    new_entry.to_csv(CURATED_CSV, mode='a', header=False, index=False)
    return jsonify({"status": "added", "entry": new_entry.to_dict(orient="records")[0]})


@app.route("/api/curated/<int:idx>", methods=["PUT"])
def update_curated(idx):
    """Update a curated portfolio entry."""
    ensure_curated_db()
    try:
        df = pd.read_csv(CURATED_CSV)
        if idx < 0 or idx >= len(df):
            return jsonify({"error": "Index out of range"}), 404

        data = request.json or {}
        for field in ['Company', 'Job_Type_or_Title', 'Location', 'Source', 'Link', 'Notes', 'Status']:
            key = field.lower().replace('job_type_or_title', 'title')
            if key in data:
                df.at[idx, field] = data[key]

        df.to_csv(CURATED_CSV, index=False)
        return jsonify({"status": "updated"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/curated/<int:idx>", methods=["DELETE"])
def delete_curated(idx):
    """Remove a curated portfolio entry."""
    ensure_curated_db()
    try:
        df = pd.read_csv(CURATED_CSV)
        if idx < 0 or idx >= len(df):
            return jsonify({"error": "Index out of range"}), 404

        df = df.drop(idx).reset_index(drop=True)
        df.to_csv(CURATED_CSV, index=False)
        return jsonify({"status": "deleted"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ──────────────────────── STARTUP ────────────────────────

if __name__ == "__main__":
    # Seed geocode cache from existing data
    seed_geocode_cache_from_csv()
    ensure_curated_db()

    print("GeoEco JobScout starting at http://localhost:5000")
    app.run(debug=True, port=5000)
