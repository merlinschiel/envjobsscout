# Environmental Job Scout

Environmental Job Scout is a tool for students of environmental sciences to find internships and jobs in Germany. It scrapes jobs from different sources using customisable search terms and gives you the option to create your own database of jobs or companies you might consider in your future. You can save scraped jobs or add your own ideas to the database.

## Job Sources

The application pulls current job listings from the following platforms:
- [greenjobs.de](https://www.greenjobs.de/)
- [Jobverde](https://www.jobverde.de/)
- [GoodJobs](https://goodjobs.eu/)

## Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/merlinschiel/envjobsscout
   cd envjobsscout
   ```

2. **Install dependencies:**
   Using [uv](https://github.com/astral-sh/uv) is recommended to manage the project dependencies and virtual environment:
   ```bash
   # Install uv (if not already installed)
   curl -LsSf https://astral.sh/uv/install.sh | sh

   # Sync dependencies and set up the virtual environment
   uv sync
   ```

   *(Alternative: If you prefer standard pip, run `pip install -r requirements.txt`)*

## Run the App

Start the application:
```bash
uv run app.py
```

*(Alternative: `python app.py` if using a traditional virtual environment)*

Open your web browser and navigate to `http://localhost:5000` (or the port shown in your terminal). 



