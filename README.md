# University ETL Agent

An automated, resilient ETL (Extract, Transform, Load) pipeline designed to dynamically discover, scrape, and validate university admissions and financial data. Built with a focus on fault tolerance and strict data quality enforcement.

## ⚙️ Core Architecture

* **Heuristic Crawler:** A custom DOM-scoring web scraper that evaluates and prioritizes high-value links (e.g., `/tuition`, `/admissions`) while strictly adhering to a depth limit of 2.
* **Strict Schema Validation:** Enforces data integrity using `Pydantic`. Automatically handles type casting, `Enum` validation, and deduplication of records.
* **Resilient Orchestration:** Wraps external network and LLM endpoints in exponential backoff protocols using `tenacity` to ensure graceful degradation rather than batch-job failures.
* **Automated QA:** Includes a comprehensive `unittest` suite to continuously verify link scoring precision and data quality filters.

## 🛠️ Tech Stack

* **Language:** Python 3.9+
* **Data Validation:** Pydantic (with `pydantic[email]`)
* **Web Scraping:** BeautifulSoup4, Requests
* **Resilience:** Tenacity

## 🚀 Quick Start

**1. Clone the repository:**
Using SSH:
```bash
git clone git@github.com:akash8534/university-etl-agent.git
cd university-etl-agent
