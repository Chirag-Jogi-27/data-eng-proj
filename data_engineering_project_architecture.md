# Data Engineering Project — Research, Architecture & Implementation Plan

---

## TABLE OF CONTENTS

1. [Candidate Project Ideas](#1-candidate-project-ideas)
2. [Data Source Research & Verification](#2-data-source-research--verification)
3. [Candidate Comparison Matrix](#3-candidate-comparison-matrix)
4. [Selected Project](#4-selected-project--us-domestic-aviation-operations-analytics)
5. [Free-Tier Validation](#5-free-tier-validation)
6. [Business Problem Definition](#6-business-problem-definition)
7. [Downstream Consumers](#7-downstream-consumers)
8. [Complete Architecture](#8-complete-architecture)
9. [Medallion Architecture — Table Definitions](#9-medallion-architecture--table-definitions)
10. [Engineering Concept Map](#10-engineering-concept-map)
11. [Data Volume & Scale Strategy](#11-data-volume--scale-strategy)
12. [Implementation Roadmap](#12-implementation-roadmap)

---

## 1. CANDIDATE PROJECT IDEAS

I researched 8 candidate domains. For each, I evaluated against your criteria: real-world realism, multiple heterogeneous sources, diverse formats, sufficient volume, historical depth, SCD potential, data quality challenges, and free-tier feasibility.

---

### Candidate 1: US Domestic Aviation Operations Analytics ⭐ RECOMMENDED

| Dimension | Detail |
|---|---|
| **Domain** | Aviation / Transportation |
| **Business Problem** | Airline operations company needs to understand flight delay patterns, root causes, seasonal trends, and the impact of weather on on-time performance |
| **Data Sources** | 4 sources: BTS On-Time Performance (CSV bulk), Open-Meteo Historical Weather API (REST), BTS Lookup Tables (CSV reference), Fuel prices from EIA API (REST) |
| **Source Types** | 2× CSV bulk download, 2× REST API |
| **Formats** | CSV, JSON (API responses) |
| **Volume** | ~500K-700K flight records/month; 6M+ rows/year; 100+ columns available |
| **Historical Data** | Available from 1987 to present (~37 years) |
| **Incremental Processing** | Monthly file releases, daily weather API pulls, quarterly fuel prices |
| **SCD Potential** | Airline dimension (mergers, name changes, code reassignments); Airport dimension (terminal changes, service zone changes) |
| **Data Quality Challenges** | Missing delay cause data (only reported for delays ≥15 min), cancelled flights with null times, diverted flight records, timezone handling, weather station mapping |
| **Gold Layer Potential** | Daily/monthly delay KPIs, route performance, weather-correlated delays, carrier rankings, seasonal patterns, fuel cost impact |
| **Downstream Consumers** | BI dashboards, operations analysts, data scientists (delay prediction features) |
| **Free-Tier Feasibility** | ✅ Excellent — all sources are free, public domain |
| **Concepts Covered** | All 30+ listed concepts |
| **Difficulty** | Medium-High |

---

### Candidate 2: NYC Urban Mobility Analytics

| Dimension | Detail |
|---|---|
| **Domain** | Transportation / Mobility |
| **Business Problem** | City transportation authority analyzing taxi demand patterns, zone performance, and weather impact on ridership |
| **Data Sources** | NYC TLC Taxi Trip Data (Parquet), Citibike Trip Data (CSV), Open-Meteo Weather API, NYC Taxi Zone Lookup (CSV) |
| **Source Types** | 2× File download, 1× REST API, 1× Reference CSV |
| **Formats** | Parquet, CSV, JSON |
| **Volume** | ~20M+ yellow taxi trips/month; billions of total historical rows |
| **Historical Data** | 2009 to present |
| **SCD Potential** | Limited — zone boundaries rarely change, taxi fare structure changes |
| **Data Quality Challenges** | GPS outliers, fare anomalies, missing location IDs, trip duration outliers |
| **Free-Tier Feasibility** | ⚠️ Moderate — data volume is extremely large, even a single month's file can be 500MB+, challenging for free-tier storage |
| **Difficulty** | Medium |

> **Why not selected:** Volume is *too* large for free-tier cloud storage (a single year can be 10+ GB in Parquet). SCD opportunities are limited. The taxi zone dimension is mostly static. Would require aggressive subsetting that reduces engineering learning.

---

### Candidate 3: Energy Market Analytics

| Dimension | Detail |
|---|---|
| **Domain** | Energy / Utilities |
| **Business Problem** | Energy trading firm analyzing electricity generation, fuel prices, and weather correlations to forecast energy costs |
| **Data Sources** | EIA API (electricity generation, fuel prices), Open-Meteo Weather API, NOAA Climate Data, EIA Bulk CSV downloads |
| **Volume** | Moderate — thousands of time-series records, not millions |
| **SCD Potential** | Power plant status changes, fuel type transitions |
| **Free-Tier Feasibility** | ✅ Good — APIs are free, data volume is manageable |
| **Difficulty** | Medium |

> **Why not selected:** Data volume is too low for meaningful Spark/Delta experience. Most processing could be done with pandas — doesn't justify Spark. Fact tables would be narrow. Less intuitive business problem for portfolio demonstration.

---

### Candidate 4: E-Commerce / Retail Analytics

| Dimension | Detail |
|---|---|
| **Domain** | E-Commerce |
| **Business Problem** | Online retailer analyzing customer behavior, product performance, inventory |
| **Data Sources** | Would need synthetic/simulated data or Kaggle datasets |
| **Volume** | Controllable via simulation |
| **SCD Potential** | ✅ Excellent — product price changes, customer address changes, category hierarchy changes |
| **Free-Tier Feasibility** | ✅ Good |
| **Difficulty** | Medium |

> **Why not selected:** No real external API or public data source. Would be entirely simulated data, which defeats the requirement of "real-world data arriving from external sources through different channels." Loses the learning of dealing with real API quirks, real schema inconsistencies, and real data quality problems.

---

### Candidate 5: Sports Analytics (MLB/NBA)

| Dimension | Detail |
|---|---|
| **Domain** | Sports |
| **Data Sources** | StatsAPI (free, no key), historical CSV datasets |
| **Volume** | Moderate — thousands of games, millions of play-by-play records |
| **SCD Potential** | Player team changes, stadium changes |
| **Free-Tier Feasibility** | ⚠️ Uncertain — API terms may restrict automated bulk access |

> **Why not selected:** API terms of service for major sports leagues often restrict automated data collection. Volume is moderate. Domain is niche — less impressive for a data engineering portfolio.

---

### Candidate 6: Stock Market / Financial Analytics

| Dimension | Detail |
|---|---|
| **Domain** | Finance |
| **Data Sources** | Alpha Vantage (25 req/day free), Yahoo Finance, SEC filings |
| **Volume** | Could be large with tick data, but free APIs severely limit it |
| **Free-Tier Feasibility** | ❌ Poor — 25 requests/day makes meaningful batch ingestion impractical |

> **Why not selected:** Free API limits (25 calls/day for Alpha Vantage) make it impossible to build a realistic batch ingestion pipeline. You'd spend weeks just collecting enough data. Commercial use restrictions exist.

---

### Candidate 7: Government/Public Infrastructure (Bridge/Highway)

| Dimension | Detail |
|---|---|
| **Domain** | Infrastructure |
| **Data Sources** | National Bridge Inventory, Federal Highway data |
| **Volume** | ~600K bridge records (relatively static) |
| **SCD Potential** | Bridge condition ratings change over years |

> **Why not selected:** Data is too static — updates happen annually. Cannot demonstrate incremental processing, late-arriving data, or daily batch patterns. Very limited API sources.

---

### Candidate 8: Healthcare Analytics

| Dimension | Detail |
|---|---|
| **Domain** | Healthcare |
| **Data Sources** | CMS Medicare data, CDC datasets |
| **Volume** | Moderate to large |
| **SCD Potential** | Provider enrollment changes, facility status changes |

> **Why not selected:** Sensitive domain — even public data requires careful handling. Data access is bureaucratic. Not ideal for a learning project. Schema is complex but domain expertise is required to build meaningful transformations.

---

## 2. DATA SOURCE RESEARCH & VERIFICATION

### Selected Project Sources (Aviation Operations Analytics)

---

### Source A — BTS Airline On-Time Performance Data (BULK CSV)

| Attribute | Detail |
|---|---|
| **Official Name** | Airline On-Time Performance Data |
| **Provider** | Bureau of Transportation Statistics (BTS), US Department of Transportation |
| **URL** | https://www.transtats.bts.gov/ |
| **Data Type** | Flight-level operational records |
| **Format** | CSV (ZIP compressed) |
| **Approx Size** | ~100-200 MB per month (CSV); ~600K-700K rows per month |
| **Historical Availability** | 1987 to present (37+ years) |
| **Update Frequency** | Monthly (with ~2 month lag) |
| **API Limits** | N/A — bulk file download |
| **Authentication** | None required |
| **Free** | ✅ Yes — US government public data |
| **Commercial Use** | ✅ Unrestricted — public domain |
| **Automated Ingestion** | ✅ Allowed — direct URL download |
| **Data Changes Over Time** | ✅ Yes — monthly new data, schema has evolved (new columns added over the years, e.g., `cbd_congestion_fee` added in 2025) |
| **Incremental Ingestion** | ✅ Yes — new monthly file each period |
| **Key Columns** | `FlightDate`, `Reporting_Airline`, `Origin`, `Dest`, `DepDelay`, `ArrDelay`, `CarrierDelay`, `WeatherDelay`, `NASDelay`, `SecurityDelay`, `LateAircraftDelay`, `Cancelled`, `Diverted`, `Distance`, `AirTime`, `TaxiOut`, `TaxiIn`, `CRSDepTime`, `CRSArrTime`, `ActualElapsedTime` |
| **Data Quality Issues** | Delay cause columns are NULL when flight is <15 min late; cancelled flights have NULL arrival/departure actuals; timezone inconsistencies (times reported in local time); diverted flights have unusual timing patterns |

---

### Source B — Open-Meteo Historical Weather API (REST API)

| Attribute | Detail |
|---|---|
| **Official Name** | Open-Meteo Historical Weather API |
| **Provider** | Open-Meteo (open-source weather API) |
| **URL** | https://open-meteo.com/en/docs/historical-weather-api |
| **API Endpoint** | `https://archive-api.open-meteo.com/v1/archive` |
| **Data Type** | Hourly/daily weather observations by lat/lon |
| **Format** | JSON (API response) |
| **Approx Size** | Small per request (~1-5 KB); total depends on number of airports × date range |
| **Historical Availability** | 1940 to present |
| **Update Frequency** | Daily (historical archive updated daily) |
| **API Limits** | 10,000 calls/day, 5,000/hour, 600/minute (weighted by complexity) |
| **Authentication** | ✅ None required — no API key needed |
| **Free** | ✅ Yes — for non-commercial use (CC BY 4.0) |
| **Commercial Use** | ❌ Requires paid plan |
| **Automated Ingestion** | ✅ Allowed within rate limits |
| **Key Variables** | `temperature_2m`, `precipitation`, `windspeed_10m`, `visibility`, `snowfall`, `weathercode`, `cloudcover` |
| **Data Quality Issues** | Weather stations have coverage gaps; some variables are modeled (not direct observation); coordinate precision affects which station is used |

---

### Source C — BTS Lookup/Reference Tables (CSV FILES)

| Attribute | Detail |
|---|---|
| **Official Name** | BTS Transportation Statistics Lookup Tables |
| **Provider** | Bureau of Transportation Statistics |
| **URL** | https://www.transtats.bts.gov/ (via "Get Lookup Table" links); also on Kaggle |
| **Data Type** | Reference/dimension data — airline codes, airport details |
| **Format** | CSV |
| **Approx Size** | Small — a few hundred to a few thousand rows each |
| **Key Tables** | `L_AIRLINE_ID` (airline codes → names), `L_AIRPORT` (airport codes → city, state), `L_AIRPORT_ID` (airport IDs), `L_CITY_MARKET_ID`, `L_CANCELLATION` (cancellation reason codes) |
| **Historical Availability** | Maintained over time — codes are added, some deprecated |
| **Update Frequency** | Irregular — updated when codes change |
| **Authentication** | None |
| **Free** | ✅ Yes — public domain |
| **SCD Relevance** | ✅ **Critical** — Airlines merge (e.g., US Airways → American Airlines), codes get reassigned, airports change names/cities |
| **Data Quality Issues** | Code reuse over time, inconsistent formatting, some lookup tables have changed column names |

---

### Source D — EIA Energy/Fuel Price Data (REST API)

| Attribute | Detail |
|---|---|
| **Official Name** | EIA Open Data API — Petroleum & Jet Fuel Prices |
| **Provider** | U.S. Energy Information Administration |
| **URL** | https://www.eia.gov/opendata/ |
| **Data Type** | Time-series — jet fuel spot prices, refinery prices |
| **Format** | JSON (API response) |
| **Approx Size** | Very small — weekly/monthly price points |
| **Historical Availability** | 1990 to present |
| **Update Frequency** | Weekly/Monthly |
| **API Limits** | Generous — no strict published rate limit for standard use |
| **Authentication** | ✅ Free API key required (instant registration) |
| **Free** | ✅ Yes — US government data |
| **Commercial Use** | ✅ Unrestricted |
| **Key Data Points** | Kerosene-type jet fuel spot price, US Gulf Coast; Weekly and monthly series |
| **Data Quality Issues** | Missing data points on holidays; price spikes require validation; units may change between series |

---

## 3. CANDIDATE COMPARISON MATRIX

Scoring: 1 (Poor) → 5 (Excellent)

| Criterion | Aviation ⭐ | NYC Mobility | Energy | E-Commerce | Sports | Finance |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| Real-world realism | 5 | 5 | 4 | 3 | 3 | 4 |
| Data volume | 5 | 5 | 2 | 3 | 3 | 2 |
| Source diversity | 5 | 4 | 4 | 2 | 3 | 2 |
| API availability | 5 | 3 | 4 | 1 | 3 | 1 |
| Historical data depth | 5 | 5 | 4 | 1 | 4 | 2 |
| Incremental processing | 5 | 5 | 3 | 3 | 4 | 2 |
| SCD opportunities | 5 | 2 | 3 | 5 | 4 | 2 |
| Data quality challenges | 5 | 4 | 3 | 2 | 3 | 3 |
| Spark relevance | 5 | 5 | 2 | 3 | 3 | 2 |
| Delta Lake relevance | 5 | 5 | 3 | 4 | 3 | 3 |
| Airflow relevance | 5 | 4 | 4 | 3 | 3 | 3 |
| Cloud storage relevance | 5 | 5 | 3 | 3 | 3 | 2 |
| Reporting potential | 5 | 4 | 4 | 4 | 4 | 4 |
| Free-tier feasibility | 5 | 3 | 5 | 4 | 3 | 1 |
| Overall learning value | 5 | 4 | 3 | 3 | 3 | 2 |
| **TOTAL** | **75** | **63** | **51** | **44** | **49** | **35** |

---

## 4. SELECTED PROJECT — US Domestic Aviation Operations Analytics

### Why This Project Wins

1. **Real external data sources** — not simulated. You will deal with real government CSV files, real weather API responses, real fuel price data. Each source has its own quirks.

2. **Four heterogeneous sources** — two bulk file downloads (CSV) and two REST APIs (JSON). This teaches you to build ingestion pipelines for fundamentally different source types.

3. **Volume sweet spot** — A single year has ~6-7 million flight records across 100+ columns. Three years gives ~20M rows. This is large enough to genuinely need Spark/Delta but small enough to fit within free-tier limits if managed carefully.

4. **Rich SCD scenarios** — Airlines merge (US Airways → American Airlines in 2015), airports change service zones, airline codes get reassigned. These are *real* historical events, not fabricated ones.

5. **Excellent data quality challenges** — Delay cause fields are only populated for flights delayed ≥15 minutes. Cancelled flights have NULL timing fields. Diverted flights have complex records. Times are in local timezone with no timezone indicator. Weather data requires coordinate-based joins. This gives you genuine data engineering problems to solve.

6. **Natural incremental processing** — BTS releases monthly files. Weather data is pulled daily. Fuel prices come weekly. Each source has a different cadence — exactly like a real company.

7. **Meaningful Gold layer** — Business questions are intuitive: "Which routes have the worst delays?", "How does weather affect on-time performance?", "Which carriers are improving?", "What's the cost impact of delays?" These drive real dashboards.

8. **100% free** — Every data source is either US government public domain or free/open-source API with no key required.

9. **Portfolio impressive** — This is a domain every interviewer understands. "I built a data platform that ingests flight operations data from 4 sources, applies weather correlation, tracks airline dimension changes over time, and produces operational KPI dashboards" is a compelling story.

---

## 5. FREE-TIER VALIDATION

### Databricks Free Edition

| Aspect | Status |
|---|---|
| **Availability** | ✅ Available (replaced Community Edition in 2025) |
| **Compute** | Serverless only; fair-usage quota (may be throttled if exceeded) |
| **Delta Lake** | ✅ Full support — MERGE, time travel, schema evolution |
| **Unity Catalog** | ✅ One workspace, one metastore |
| **File Upload** | ✅ Via Unity Catalog Volumes (up to 5 GB per upload through UI) |
| **SQL Warehouse** | ✅ One 2X-Small SQL warehouse |
| **Jobs** | ✅ Up to 5 concurrent job tasks |
| **Notebooks** | ✅ Supported |
| **Outbound Internet** | ⚠️ **RESTRICTED** to trusted domains only — cannot call arbitrary external APIs from within Databricks |
| **DBFS** | ❌ Deprecated — use Volumes instead |

> [!IMPORTANT]
> **Critical Architecture Impact:** Databricks Free Edition **cannot reach external APIs** (Open-Meteo, EIA) directly. This means **API ingestion must happen outside Databricks** — on your local machine or in a cloud function. Data is then uploaded to cloud storage or Databricks Volumes. This is actually **more realistic** — in production, ingestion and processing are typically separate concerns.

### Cloud Platform Configuration (AWS Free Tier Optimization)

| Feature | AWS Entitlement | SkyOps Implementation Strategy | Cost |
|---|---|---|---|
| **Compute** | 750 hours/month of `t2.micro` (or `t3.micro` in US-East/West/EU) | Deploy **one** dedicated instance for Airflow and Python ingestion scripts. Run without Docker using a native python `venv` to minimize memory usage. | **$0.00** |
| **Block Storage** | 30 GB of gp2/gp3 SSD volume | Provision a single **20 GB root EBS drive**. Set up automated bash scripts to clear out downloaded CSV data files immediately after processing. | **$0.00** |
| **Object Storage** | 5 GB Standard S3 storage | Host the raw Bronze, Silver, and Gold Delta tables. Compress raw source CSV data into Parquet formats locally on EC2 before shipping to S3 to save space. | **$0.00** |
| **Database** | N/A (Not using paid RDS/Redshift) | Leverage **Databricks Free Edition SQL Warehouse** for serverless ad-hoc analytics queries rather than AWS managed databases. | **$0.00** |
### AWS Cost Control Strategy

| Component | Where It Runs | Cost Optimization Mechanism |
|---|---|---|
| **API Ingestion Engine** | AWS EC2 (`t2.micro`) | Native Python virtual environment execution. SequentialExecutor setup to restrict background memory leaks. |
| **Landing Storage Zone** | Amazon S3 | Hard programmatic **14-day lifecycle expiration rule** applied directly to `s3://skyops-data-lake/landing/` to drop raw source objects after they are successfully parsed into Bronze Delta structures. |
| **Delta Processing Layer** | Databricks Free Tier | Direct programmatic integration between EC2 push scripts and **Unity Catalog Volumes**. Data writes do not leave the free tier workspace metastore footprint. |
| **Infrastructure Guardrails** | AWS CloudWatch | Hard **AWS Budget Email Notification rule** configured at **$0.01 USD** to alert the engineering framework immediately if any tracking threshold or API baseline is breached. |
---

## 6. BUSINESS PROBLEM DEFINITION

### Company Scenario

**SkyOps Analytics** is a fictional aviation operations consulting firm that provides on-time performance intelligence to airline executives, airport operators, and government regulators.

They have contracts with multiple domestic US carriers to provide:
- Monthly operational performance reports
- Delay root cause analysis
- Weather impact assessments
- Route profitability indicators (via fuel cost correlation)
- Historical trend analysis with carrier-level benchmarking

### Current Situation (Before Our Platform)

Data arrives from multiple disconnected sources:
- **BTS** publishes monthly CSV files (with a 2-month lag) containing every domestic flight's operational data
- **Weather observations** must be pulled from Open-Meteo by airport coordinates to correlate delays with weather conditions
- **Fuel prices** are tracked via the EIA API to understand cost impacts
- **Reference tables** (airline codes, airport details, cancellation codes) are maintained as CSV lookups that change infrequently but without notification

### Current Problems

| Problem | Real-World Impact |
|---|---|
| **Data scattered across systems** | Analysts manually download BTS files, weather data, and fuel prices separately |
| **No historical tracking** | When an airline merges (e.g., US Airways → American), old reports break because the carrier code changed |
| **Duplicate processing** | If someone re-downloads the same month's file, it gets processed again, doubling row counts |
| **Missing data handling** | Cancelled flights are dropped entirely, distorting delay statistics |
| **Schema changes** | BTS added a `cbd_congestion_fee` column in 2025 — existing pipelines broke |
| **No data quality checks** | Flights with impossible departure delays (e.g., -1440 minutes) pollute aggregates |
| **Weather correlation is manual** | Analysts manually look up weather for a specific airport/date — no systematic join |
| **No audit trail** | Nobody knows when data was last refreshed or how many records were processed |
| **Inconsistent timezone handling** | BTS reports times in local time without timezone indicator; comparing flights across timezones is wrong |
| **Late-arriving data** | BTS occasionally revises previous months' data; these corrections are never picked up |

### Business Questions the Platform Must Answer

1. **What is the on-time performance by carrier, by month?** (Gold: carrier performance fact)
2. **Which routes have the highest average delays?** (Gold: route performance fact)
3. **What percentage of delays are weather-related vs. carrier-caused?** (Gold: delay cause analysis)
4. **How does weather at origin/destination airports correlate with delays?** (Gold: weather impact fact)
5. **Which airports have the worst departure delays?** (Gold: airport performance fact)
6. **How has carrier performance trended over time?** (Gold: trend analysis, SCD2)
7. **What is the estimated fuel cost impact of delays?** (Gold: cost impact fact)
8. **Are there seasonal delay patterns?** (Gold: seasonal aggregation)
9. **Which carriers/routes are improving or deteriorating?** (Gold: period-over-period comparison)
10. **What happened to flights from merged/acquired airlines?** (Gold: SCD2 dimension history)

---

## 7. DOWNSTREAM CONSUMERS

### Consumer 1: Operations BI Dashboard (Primary)

**User:** VP of Operations at partner airlines

**Needs:**
- Daily/monthly KPI dashboard showing on-time %, average delay, cancellation rate
- Drill-down by carrier → route → airport → date
- Weather overlay showing delay correlation
- Trend charts (month-over-month, year-over-year)
- Top-10 worst routes / best routes

**Gold tables consumed:**
- `gold.fact_daily_flight_performance`
- `gold.fact_route_performance`
- `gold.fact_weather_delay_correlation`
- `gold.dim_carrier` (with SCD2 history)
- `gold.dim_airport`
- `gold.dim_date`

---

### Consumer 2: Data Analyst

**User:** Internal analyst running ad-hoc SQL queries

**Needs:**
- Clean, well-modeled star schema
- Historical dimension data (SCD2) to answer "what was the carrier's name when this flight operated?"
- SQL-accessible tables with proper data types, no nulls in keys
- Ability to query across all layers (Bronze for debugging, Silver for detailed analysis, Gold for reporting)

**Gold tables consumed:** All fact and dimension tables via Databricks SQL warehouse

---

### Consumer 3: Data Scientist (Future)

**User:** Predictive analytics team building delay prediction models

**Needs:**
- Feature-ready datasets: flight features + weather features + fuel prices, joined at the right granularity
- Historical completeness — no dropped records
- Clean nulls and consistent encoding

**Gold table consumed:**
- `gold.fact_flight_features` (denormalized, ML-ready)

---

> [!NOTE]
> Every Gold table is designed to serve a specific consumer need. We do not create Gold tables speculatively.

---

## 8. COMPLETE ARCHITECTURE

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        EXTERNAL DATA SOURCES                            │
├─────────────────┬──────────────────┬────────────────┬───────────────────┤
│  BTS On-Time    │  Open-Meteo      │  BTS Lookup    │  EIA Fuel Price   │
│  Performance    │  Weather API     │  Tables        │  API              │
│  (Monthly CSV)  │  (REST/JSON)     │  (CSV files)   │  (REST/JSON)      │
└────────┬────────┴────────┬─────────┴───────┬────────┴──────────┬────────┘
         │                 │                 │                   │
         ▼                 ▼                 ▼                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                 INGESTION LAYER (AWS EC2 - Python)                      │
│                                                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌─────────────┐ │
│  │ BTS CSV      │  │ Weather API  │  │ Lookup CSV   │  │ EIA API     │ │
│  │ Downloader   │  │ Collector    │  │ Downloader   │  │ Collector   │ │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬──────┘ │
│         │                 │                 │                  │        │
│         ▼                 ▼                 ▼                  ▼        │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                EC2 LANDING ZONE (filesystem)                    │    │
│  │   data/landing/bts/YYYY-MM/                                    │    │
│  │   data/landing/weather/YYYY-MM-DD/                             │    │
│  │   data/landing/lookups/                                        │    │
│  │   data/landing/fuel/YYYY-MM/                                   │    │
│  └──────────────────────────┬──────────────────────────────────────┘    │
└─────────────────────────────┼──────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                  CLOUD STORAGE (Amazon S3 — 5 GB Free)                  │
│                                                                         │
│   s3://skyops-data-lake/                                                │
│   ├── landing/          ← Raw files as received                        │
│   ├── bronze/           ← Delta tables (raw + metadata)                │
│   ├── silver/           ← Delta tables (cleaned, validated)            │
│   ├── gold/             ← Delta tables (business-ready)                │
│   ├── quarantine/       ← Rejected/bad records                         │
│   └── audit/            ← Pipeline run metadata                       │
└──────────────────────────┬──────────────────────────────────────────────┘
                           │
         ┌─────────────────┴─────────────────────┐
         │                                       │
         ▼                                       ▼
┌─────────────────────┐              ┌─────────────────────────────────┐
│  DATABRICKS FREE    │              │   ALTERNATIVE: LOCAL SPARK      │
│  EDITION            │              │   (for development/testing)     │
│                     │              │                                 │
│  ┌───────────────┐  │              │   PySpark on EC2/local machine  │
│  │ Unity Catalog │  │              │   Delta Lake OSS                │
│  │ Volumes       │  │              │   For rapid iteration without   │
│  └───────┬───────┘  │              │   Databricks quota limits       │
│          │          │              └─────────────────────────────────┘
│          ▼          │
│  ┌───────────────┐  │
│  │ BRONZE Layer  │  │
│  │ Delta Tables  │  │
│  └───────┬───────┘  │
│          │          │
│          ▼          │
│  ┌───────────────┐  │
│  │ SILVER Layer  │  │
│  │ Delta Tables  │  │
│  └───────┬───────┘  │
│          │          │
│          ▼          │
│  ┌───────────────┐  │
│  │ GOLD Layer    │  │
│  │ Delta Tables  │  │
│  └───────────────┘  │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     DOWNSTREAM CONSUMERS                                │
│                                                                         │
│   ┌──────────────┐   ┌──────────────┐   ┌────────────────────────┐     │
│   │ Databricks   │   │ AWS Athena   │   │ Future: BI Tool        │     │
│   │ SQL Queries  │   │ (optional)   │   │ (Metabase/Superset)    │     │
│   └──────────────┘   └──────────────┘   └────────────────────────┘     │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│                     ORCHESTRATION (Apache Airflow — AWS EC2)             │
│                                                                         │
│   DAG: skyops_daily_pipeline                                            │
│   ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌────────┐  │
│   │ Ingest   │→│ Upload   │→│ Bronze   │→│ Silver   │→│ Gold   │  │
│   │ Sources  │  │ to S3    │  │ Process  │  │ Process  │  │Process │  │
│   └──────────┘  └──────────┘  └──────────┘  └──────────┘  └────────┘  │
│        │                            │              │            │       │
│        ▼                            ▼              ▼            ▼       │
│   ┌──────────┐              ┌──────────┐   ┌──────────┐  ┌────────┐   │
│   │ Validate │              │ Quality  │   │Quarantine│  │ Audit  │   │
│   │ Source   │              │ Checks   │   │ Bad Recs │  │ Log    │   │
│   └──────────┘              └──────────┘   └──────────┘  └────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
```

### Component Justification

| Component | Why It Exists | What Happens If Removed |
|---|---|---|
| **EC2 Ingestion Layer** | Databricks Free Edition can't access external APIs. Ingestion must happen outside on EC2. This is the production pattern — ingestion and processing are decoupled. | Cannot get data from APIs. Pipeline breaks at the source. |
| **EC2 Landing Zone** | Provides a staging area before S3 upload. Enables decoupled development/testing. Acts as a cache to avoid re-downloading. | Every run requires re-downloading from source. No decoupled development possible. |
| **Amazon S3 (Storage)** | Provides durable, centralized storage accessible by Databricks. Separates storage from compute (lakehouse principle). | No persistent storage. Data lost when compute stops. Can't share data between processing runs. |
| **Databricks** | Provides managed Spark, Delta Lake, Unity Catalog. Production-grade processing engine. | Must run Spark locally, losing the production-like experience with managed infrastructure, Unity Catalog, SQL warehouse. |
| **Bronze Layer** | Preserves raw data exactly as received, with metadata. Enables reprocessing from source-of-truth. | If Silver processing has a bug, you cannot reprocess — you must re-download from source. |
| **Silver Layer** | Clean, validated, deduplicated data. The "single source of truth" for the organization. | Gold layer must handle all cleansing, making it complex and fragile. Business users query dirty data. |
| **Gold Layer** | Business-ready dimensional model. Optimized for specific query patterns. | Analysts must write complex joins across Silver tables. No pre-computed aggregations. Slow dashboards. |
| **Quarantine** | Bad records are separated, not deleted. Enables investigation and reprocessing. | Bad data either pollutes downstream tables or is silently lost. No way to understand data quality issues. |
| **Airflow** | Orchestrates the end-to-end pipeline. Handles dependencies, retries, scheduling, alerting. | Must run each step manually. No dependency management. No automatic retries. No scheduling. No audit trail. |
| **Audit Tables** | Track what was processed, when, how many records, success/failure. | No visibility into pipeline health. Cannot debug failures. Cannot verify completeness. |

---

## 9. MEDALLION ARCHITECTURE — TABLE DEFINITIONS

### BRONZE TABLES

Bronze preserves source data with minimal transformation, adding only metadata columns.

#### `bronze.flights`
| Column | Source | Notes |
|---|---|---|
| All original BTS columns (~100+) | BTS CSV | Preserved as-is, original data types |
| `_source_file` | Metadata | Full source filename |
| `_ingestion_timestamp` | Metadata | UTC timestamp of ingestion |
| `_batch_id` | Metadata | UUID for this processing run |
| `_source_system` | Metadata | Constant: `"bts_ontime"` |
| `_record_hash` | Metadata | SHA-256 hash of key business columns for dedup |
| `_ingestion_date` | Metadata | Date partition column |

**Partitioning:** By `_ingestion_date` (date files were loaded, not flight date)
**Why partition by ingestion date at Bronze?** Because Bronze is about *when data arrived*, not when events occurred. This makes reprocessing a specific load easy (drop and reload a partition). Partitioning by flight date at Bronze would mix records from different ingestion batches.

#### `bronze.weather`
| Column | Source |
|---|---|
| `latitude`, `longitude` | API request params |
| `date`, `hour` | API response |
| `temperature_2m`, `precipitation`, `windspeed_10m`, `visibility`, `snowfall`, `weathercode`, `cloudcover` | API response |
| `_airport_code` | Metadata — which airport this was fetched for |
| `_source_system` | `"open_meteo"` |
| `_ingestion_timestamp`, `_batch_id`, `_ingestion_date` | Standard metadata |

**Partitioning:** By `_ingestion_date`

#### `bronze.fuel_prices`
| Column | Source |
|---|---|
| `period`, `value`, `unit`, `series_id` | EIA API response |
| `_source_system`, `_ingestion_timestamp`, `_batch_id`, `_ingestion_date` | Standard metadata |

**Partitioning:** By `_ingestion_date`

#### `bronze.lookups_airlines`, `bronze.lookups_airports`, `bronze.lookups_cancellation`
| Column | Source |
|---|---|
| All columns from source CSV | BTS lookup tables |
| `_source_file`, `_ingestion_timestamp`, `_batch_id`, `_source_system` | Standard metadata |
| `_file_hash` | MD5 of the entire source file (for change detection) |

**Partitioning:** None (small reference tables)

---

### SILVER TABLES

Silver contains cleaned, validated, deduplicated, type-standardized data.

#### `silver.flights`
| Column | Type | Transformation |
|---|---|---|
| `flight_date` | DATE | Cast from string, validate format |
| `reporting_airline` | STRING | Trimmed, uppercased |
| `airline_id` | INT | Validated against lookup |
| `origin_airport_code` | STRING | Standardized 3-letter code |
| `dest_airport_code` | STRING | Standardized 3-letter code |
| `origin_airport_id` | INT | Validated |
| `dest_airport_id` | INT | Validated |
| `flight_number` | STRING | Trimmed |
| `tail_number` | STRING | Trimmed, nullable |
| `scheduled_departure` | TIMESTAMP | Constructed from date + HHMM time, local tz |
| `actual_departure` | TIMESTAMP | NULL for cancelled flights |
| `scheduled_arrival` | TIMESTAMP | Constructed |
| `actual_arrival` | TIMESTAMP | NULL for cancelled flights |
| `departure_delay_min` | DOUBLE | Validated range (-120 to 1440) |
| `arrival_delay_min` | DOUBLE | Validated range |
| `carrier_delay_min` | DOUBLE | NULL if flight not delayed ≥15 min |
| `weather_delay_min` | DOUBLE | Same |
| `nas_delay_min` | DOUBLE | Same |
| `security_delay_min` | DOUBLE | Same |
| `late_aircraft_delay_min` | DOUBLE | Same |
| `is_cancelled` | BOOLEAN | Cast from 0/1 |
| `cancellation_code` | STRING | Validated against lookup |
| `is_diverted` | BOOLEAN | Cast from 0/1 |
| `distance_miles` | DOUBLE | Validated > 0 |
| `air_time_min` | DOUBLE | Validated |
| `taxi_out_min` | DOUBLE | Validated |
| `taxi_in_min` | DOUBLE | Validated |
| `is_delayed` | BOOLEAN | Derived: `arrival_delay_min >= 15` |
| `_source_batch_id` | STRING | Traceability to Bronze |
| `_processing_timestamp` | TIMESTAMP | When Silver processing ran |
| `_record_hash` | STRING | For dedup/idempotency |

**Partitioning:** By `flight_date` (YYYY-MM partitioning, monthly granularity)

**Why partition by flight_date at Silver?** Silver is the analytical source of truth. Queries filter by when the flight occurred, not when it was ingested. Monthly partitioning keeps partition count manageable (~12/year) and each partition large enough to avoid small-file problems.

**Deduplication:** Using `MERGE` with business key (`flight_date`, `reporting_airline`, `flight_number`, `origin_airport_code`, `scheduled_departure`). Same flight arriving in multiple batches is deduplicated.

**Data Quality Rules Applied:**
- `flight_date` is a valid date
- `departure_delay_min` between -120 and 1440 (impossible values quarantined)
- `distance_miles` > 0
- `origin_airport_code` ≠ `dest_airport_code`
- If `is_cancelled` = false, then `actual_departure` and `actual_arrival` must not be NULL
- If delay cause fields are populated, their sum should approximately equal `arrival_delay_min`
- Records failing these checks → `quarantine.flights`

#### `silver.weather_daily`
| Column | Transformation |
|---|---|
| `airport_code` | From `_airport_code` |
| `weather_date` | From API response date |
| `avg_temperature_c` | Daily average from hourly data |
| `max_temperature_c` | Daily max |
| `min_temperature_c` | Daily min |
| `total_precipitation_mm` | Daily sum |
| `max_windspeed_kmh` | Daily max |
| `min_visibility_m` | Daily minimum (worst visibility) |
| `total_snowfall_cm` | Daily sum |
| `dominant_weathercode` | Most frequent hourly code |
| `has_severe_weather` | Derived boolean |

**Partitioning:** By `weather_date` (monthly)

#### `silver.fuel_prices`
| Column | Transformation |
|---|---|
| `price_date` | Parsed from `period` field |
| `price_per_gallon` | From `value`, validated > 0 |
| `price_unit` | Standardized |
| `fuel_type` | Derived from series metadata |

**Partitioning:** None (small table)

#### `silver.airlines` (SCD Type 2 candidate)
| Column | Notes |
|---|---|
| `airline_id` | Business key |
| `airline_code` | IATA code (can be reassigned) |
| `airline_name` | Can change (mergers) |
| `effective_start_date` | SCD2 |
| `effective_end_date` | SCD2 (NULL or '9999-12-31' for current) |
| `is_current` | BOOLEAN |
| `version` | INT |

#### `silver.airports` (SCD Type 1)
| Column | Notes |
|---|---|
| `airport_id` | Business key |
| `airport_code` | IATA code |
| `airport_name` | Overwritten on change (SCD1) |
| `city` | Overwritten on change (SCD1) |
| `state` | Overwritten on change (SCD1) |
| `last_updated` | Timestamp of last change |

**Why SCD1 for airports but SCD2 for airlines?**
- **Airlines:** Historical accuracy matters. When US Airways merged into American Airlines, we need to know that flights before 2015 were operated by US Airways. Reporting should reflect the carrier name *at the time of the flight*. SCD2 preserves this.
- **Airports:** Airport name/city changes are cosmetic corrections, not business-critical historical events. If an airport's city name is corrected, we want all records (past and present) to reflect the correction. SCD1 is appropriate.

---

### GOLD TABLES

#### `gold.dim_carrier` (SCD2 — from silver.airlines)
Conformed dimension with surrogate key for joining to facts.

| Column | Type |
|---|---|
| `carrier_sk` | BIGINT (surrogate key) |
| `airline_id` | INT (natural key) |
| `airline_code` | STRING |
| `airline_name` | STRING |
| `effective_start_date` | DATE |
| `effective_end_date` | DATE |
| `is_current` | BOOLEAN |

#### `gold.dim_airport` (SCD1 — from silver.airports)
| Column | Type |
|---|---|
| `airport_id` | INT (natural key, used as key — small dimension) |
| `airport_code` | STRING |
| `airport_name` | STRING |
| `city` | STRING |
| `state` | STRING |

#### `gold.dim_date`
Pre-generated date dimension:
| Column | Type |
|---|---|
| `date_key` | DATE |
| `year` | INT |
| `quarter` | INT |
| `month` | INT |
| `month_name` | STRING |
| `day_of_month` | INT |
| `day_of_week` | INT |
| `day_name` | STRING |
| `is_weekend` | BOOLEAN |
| `is_holiday` | BOOLEAN (US federal holidays) |
| `week_of_year` | INT |

#### `gold.dim_route`
| Column | Type |
|---|---|
| `route_id` | STRING (origin_code || '-' || dest_code) |
| `origin_airport_id` | INT |
| `dest_airport_id` | INT |
| `origin_code` | STRING |
| `dest_code` | STRING |
| `distance_miles` | DOUBLE |

#### `gold.fact_daily_flight_performance`
Core fact table — one row per carrier-route-date.

| Column | Type |
|---|---|
| `carrier_sk` | BIGINT (FK to dim_carrier) |
| `origin_airport_id` | INT (FK to dim_airport) |
| `dest_airport_id` | INT (FK to dim_airport) |
| `flight_date` | DATE (FK to dim_date) |
| `total_flights` | INT |
| `on_time_flights` | INT |
| `delayed_flights` | INT |
| `cancelled_flights` | INT |
| `diverted_flights` | INT |
| `avg_departure_delay_min` | DOUBLE |
| `avg_arrival_delay_min` | DOUBLE |
| `total_carrier_delay_min` | DOUBLE |
| `total_weather_delay_min` | DOUBLE |
| `total_nas_delay_min` | DOUBLE |
| `total_security_delay_min` | DOUBLE |
| `total_late_aircraft_delay_min` | DOUBLE |
| `on_time_pct` | DOUBLE |
| `avg_distance_miles` | DOUBLE |
| `avg_air_time_min` | DOUBLE |

**Partitioning:** By `flight_date` (monthly)
**Clustering/Optimization:** Consider Z-ORDER on `(carrier_sk, origin_airport_id)` or Liquid Clustering if available

#### `gold.fact_weather_delay_correlation`
Joins flight performance with weather conditions.

| Column | Type |
|---|---|
| `flight_date` | DATE |
| `airport_id` | INT |
| `airport_code` | STRING |
| `avg_temperature_c` | DOUBLE |
| `total_precipitation_mm` | DOUBLE |
| `max_windspeed_kmh` | DOUBLE |
| `min_visibility_m` | DOUBLE |
| `has_severe_weather` | BOOLEAN |
| `total_departing_flights` | INT |
| `delayed_departing_flights` | INT |
| `departure_delay_pct` | DOUBLE |
| `avg_departure_delay_min` | DOUBLE |
| `total_weather_delay_min` | DOUBLE |

#### `gold.fact_monthly_carrier_kpi`
Monthly rollup for trend analysis.

| Column | Type |
|---|---|
| `year_month` | STRING (YYYY-MM) |
| `carrier_sk` | BIGINT |
| `total_flights` | BIGINT |
| `on_time_pct` | DOUBLE |
| `cancellation_rate` | DOUBLE |
| `avg_delay_min` | DOUBLE |
| `total_delay_min` | BIGINT |
| `weather_delay_pct` | DOUBLE |
| `carrier_delay_pct` | DOUBLE |
| `avg_fuel_price_per_gallon` | DOUBLE |
| `estimated_delay_fuel_cost` | DOUBLE |
| `rank_on_time` | INT |
| `mom_change_on_time_pct` | DOUBLE |

#### `gold.fact_flight_features` (ML-ready, denormalized)
| Column | Notes |
|---|---|
| `flight_date`, `carrier_code`, `origin`, `dest` | Identifiers |
| `scheduled_hour`, `day_of_week`, `month`, `is_weekend`, `is_holiday` | Temporal features |
| `distance_miles`, `historical_route_delay_avg` | Route features |
| `origin_avg_temp`, `origin_precipitation`, `origin_windspeed`, `origin_visibility` | Origin weather |
| `dest_avg_temp`, `dest_precipitation`, `dest_windspeed`, `dest_visibility` | Dest weather |
| `fuel_price_per_gallon` | Cost feature |
| `arrival_delay_min` | Target variable |
| `is_delayed` | Binary target |

---

## 10. ENGINEERING CONCEPT MAP

| Concept | Where Implemented | Why Used |
|---|---|---|
| **Schema validation** | Bronze → Silver transformation | BTS adds/removes columns over the years. Validate incoming schema against expected schema before processing. Reject records with wrong types. |
| **Schema evolution** | Bronze tables with `mergeSchema` | When BTS adds new columns (e.g., `cbd_congestion_fee`), Bronze should accept them without breaking. Silver schema is controlled. |
| **Idempotency** | All layers — `_batch_id` + `MERGE` + `_record_hash` | Running the same batch twice must not duplicate data. MERGE with business key ensures upsert behavior. File tracking table prevents re-processing same source file. |
| **SCD Type 1** | `silver.airports` → `gold.dim_airport` | Airport name/city corrections should be reflected everywhere. No need to preserve old incorrect names. |
| **SCD Type 2** | `silver.airlines` → `gold.dim_carrier` | Airline mergers are business-critical history. Must preserve the carrier name at the time each flight operated. |
| **Partitioning** | Silver/Gold fact tables by `flight_date` (monthly); Bronze by `_ingestion_date` | Enables partition pruning for date-range queries (90%+ of queries filter by date). Monthly granularity balances pruning benefit vs. partition count. |
| **Clustering / Z-Order** | `gold.fact_daily_flight_performance` on `(carrier_sk, origin_airport_id)` | Common filter/join columns. Z-ordering groups related data physically, enabling data skipping. |
| **OPTIMIZE** | All Delta tables after write operations | Compacts small files created by incremental writes. Prevents small-file problem that degrades read performance. |
| **Delta Lake ACID** | Every write operation | Ensures atomicity (partial writes don't corrupt tables), consistency (schema enforcement), isolation (concurrent reads during writes), durability (committed data is permanent). |
| **MERGE (Upsert)** | Silver tables, Gold dimension tables | Handles both inserts and updates in a single atomic operation. Core of idempotent processing and SCD implementation. |
| **Time Travel** | Debugging, audit, rollback | Query previous versions of tables. Essential for investigating data issues and recovering from bad processing. |
| **Data quality checks** | Silver transformation layer + dedicated quality framework | Null checks, range validation, referential integrity, uniqueness. Bad records quarantined, not silently dropped. |
| **Quarantine / Reject** | `quarantine.flights`, `quarantine.weather` | Bad records go here with rejection reason. Can be investigated and reprocessed after fixing source issues. |
| **Incremental processing** | Bronze → Silver (process only new `_ingestion_date` partitions); Silver → Gold (process only changed date ranges) | Avoids reprocessing entire history every run. Reduces cost and processing time. |
| **Late-arriving data** | Silver/Gold MERGE logic handles records arriving for past dates | BTS revises previous months. MERGE updates existing records if business key matches, inserts if new. Gold aggregates are recalculated for affected partitions. |
| **Deduplication** | Silver layer using `_record_hash` and business key | Same source file processed twice → no duplicate records in Silver. |
| **Backfill** | Airflow DAG with `catchup=True` + date parameterization | Can reprocess any historical date range. Idempotent design ensures safe re-execution. |
| **Airflow orchestration** | Complete DAG with task dependencies | Manages execution order (ingest → bronze → silver → gold), retries, alerting, scheduling. |
| **Retry mechanisms** | Airflow task retries (3 retries, 5-min delay); Python-level API retries with exponential backoff | API calls may fail transiently. Airflow retries handle task-level failures. |
| **Error handling** | Try/except with structured logging; dead-letter pattern for bad records | Every failure is captured with context. Pipeline doesn't silently fail. |
| **Logging** | Python `logging` module → structured JSON logs; Airflow task logs | Every pipeline run produces detailed logs. Enables debugging without re-running. |
| **Monitoring** | Audit table tracking record counts, durations, success/failure; Airflow UI | At-a-glance pipeline health. Detect anomalies (sudden drop in records = source issue). |
| **Alerting** | Airflow callbacks (on_failure, on_success); email/log-based alerts | Immediate notification when pipeline fails. Data quality threshold violations trigger alerts. |
| **Data lineage** | `_source_system`, `_source_file`, `_batch_id` columns; audit tables linking source → bronze → silver → gold | Can trace any Gold record back to its source file. Essential for debugging and compliance. |
| **Audit tables** | `audit.pipeline_runs`, `audit.file_tracking` | Track every execution: what was processed, how many records, how long, success/failure. Production support depends on this. |
| **Fact tables** | `gold.fact_daily_flight_performance`, `gold.fact_weather_delay_correlation`, `gold.fact_monthly_carrier_kpi` | Answering business questions with pre-aggregated, optimized datasets. |
| **Dimension tables** | `gold.dim_carrier` (SCD2), `gold.dim_airport` (SCD1), `gold.dim_date`, `gold.dim_route` | Conformed dimensions enabling consistent reporting. Star schema for query performance. |
| **Cost optimization** | Local processing for development; cloud only for production simulation; cleanup scripts; storage monitoring | Stay within free tiers. Don't leave resources running. |

---

## 11. DATA VOLUME & SCALE STRATEGY

### Raw Source Sizes

| Source | Per Unit | Annual Estimate | 3-Year Subset |
|---|---|---|---|
| BTS On-Time CSV | ~150 MB/month (compressed) | ~1.8 GB/year | ~5.4 GB |
| Open-Meteo Weather | ~2-5 KB per airport-day | ~50 MB for 350 airports × 365 days | ~150 MB |
| BTS Lookups | ~1 MB total (all tables combined) | ~1 MB | ~1 MB |
| EIA Fuel Prices | ~50 KB | ~50 KB | ~150 KB |

### Data Volume by Layer

| Layer | Expected Size (3-year subset) | Record Count | Files |
|---|---|---|---|
| **Landing (raw files)** | ~5-6 GB (CSV compressed) | N/A | ~36 monthly zips + API JSON files |
| **Bronze** | ~3-4 GB (Delta/Parquet compressed) | ~20M flight records + weather + fuel | ~100-200 Parquet files |
| **Silver** | ~2-3 GB (Delta, fewer columns, compressed) | ~19.5M flights (after quality filtering) + enriched | ~100 files |
| **Gold** | ~500 MB (aggregated, fewer rows) | ~2-5M aggregated rows across all fact tables | ~50 files |
| **Total Cloud Storage** | ~8-12 GB | | |

> [!WARNING]
> **GCS free tier is 5 GB.** With a full 3-year dataset, we will exceed the 5 GB limit. This means we must be strategic.

### Scale Levels

#### Level 1 — Development (Local Only)
| Aspect | Detail |
|---|---|
| **Data** | 1-2 months of BTS data (~1.2M rows) |
| **Processing** | Local PySpark + Delta Lake OSS |
| **Storage** | Local filesystem only |
| **Purpose** | Build and debug all transformations rapidly |
| **Cloud cost** | $0 |

#### Level 2 — Production Simulation (Cloud)

| Aspect | Detail |
|---|---|
| **Data** | 6-12 months of BTS data (~4-7M rows) |
| **Processing** | Databricks Free Edition |
| **Storage** | Amazon S3 (strictly optimized within the 5 GB free baseline — push only compressed Delta tables, do not retain raw uncompressed CSV histories long term) |
| **Purpose** | Experience enterprise AWS S3 configuration, IAM policies, Bucket controls, and Databricks SQL Engine integration. |
| **Strategy** | Download files locally to EC2, instantly compress to Parquet, upload to S3, and process through the Medallion pipeline within Databricks Volumes. |

#### Level 3 — Stress Test (Optional)
| Aspect | Detail |
|---|---|
| **Data** | 3+ years (~20M+ rows) or synthetic expansion to 100M+ rows |
| **Processing** | Local PySpark for volume testing; Databricks for Delta testing |
| **Purpose** | Experience partition management, OPTIMIZE, file compaction, query optimization |
| **Strategy** | Use Python data generator to create realistic but synthetic expanded dataset |

### What Changes Between Levels

| Capability | Level 1 | Level 2 | Level 3 |
|---|---|---|---|
| Spark execution plans | ✅ Visible | ✅ Visible + Spark UI | ✅ Visible + optimization needed |
| Partition pruning | Visible but fast | Measurably faster | Clearly necessary |
| OPTIMIZE impact | Minimal | Noticeable | Critical |
| Small-file problem | Won't appear | May appear | Definitely appears |
| Memory pressure | None | Minimal | Possible |
| Delta table versions | Works | Works | Need VACUUM |
| Incremental vs. full | Both fast | Incremental clearly faster | Full load unacceptable |

---

## 12. IMPLEMENTATION ROADMAP

### Phase 0: Business Understanding & Project Setup *(You are here)*
- ✅ Define business problem
- ✅ Research data sources
- ✅ Design architecture
- ✅ Define tables and transformations
- [ ] Create Git repository structure
- [ ] Set up Python environment
- [ ] Create `.env.example`, `.gitignore`, `requirements.txt`

### Phase 1: Environment Setup
- [ ] Install Python, PySpark (local), Delta Lake OSS
- [ ] Install Airflow locally (pip install or Docker)
- [ ] Sign up for Databricks Free Edition
- [ ] Sign up for GCP (or set up BigQuery Sandbox)
- [ ] Create GCS bucket (if using GCP free tier)
- [ ] Register for EIA API key
- [ ] Verify all access — can you download BTS data? Call Open-Meteo? Call EIA?

### Phase 2: Ingestion Layer
- [ ] BTS CSV downloader (Python) — download monthly files with retry logic
- [ ] Open-Meteo weather API collector — fetch daily weather for airport coordinates
- [ ] EIA fuel price collector — fetch jet fuel price time series
- [ ] BTS lookup table downloader — download reference CSVs
- [ ] Local landing zone directory structure
- [ ] File tracking (which files have been downloaded/processed)
- [ ] Error handling + logging for each ingestion source
- [ ] Unit tests for ingestion functions

### Phase 3: Bronze Layer
- [ ] Define explicit schemas (StructType) for each source
- [ ] Build Bronze ingestion notebooks/scripts
- [ ] Add metadata columns (`_source_file`, `_ingestion_timestamp`, `_batch_id`, etc.)
- [ ] Implement schema validation (what happens when schema changes?)
- [ ] Write to Delta tables with schema evolution enabled
- [ ] Implement file tracking table (`audit.processed_files`)
- [ ] Test idempotency: process same file twice → verify no duplicates
- [ ] Test schema evolution: add a column to source → verify Bronze handles it

### Phase 4: Silver Layer — Core Transformations
- [ ] Define Silver schemas (explicit, controlled)
- [ ] Implement flights transformation: type casting, standardization, derived columns
- [ ] Implement data quality rules (null checks, range validation, referential integrity)
- [ ] Build quarantine mechanism — bad records → `quarantine.flights`
- [ ] Implement deduplication using `MERGE` with business key
- [ ] Implement incremental processing (only process new Bronze partitions)
- [ ] Implement weather daily aggregation (hourly → daily)
- [ ] Implement fuel price standardization

### Phase 5: Silver Layer — SCD
- [ ] Implement SCD Type 2 for airlines dimension
- [ ] Implement SCD Type 1 for airports dimension
- [ ] Test SCD2: simulate an airline name change → verify history preserved
- [ ] Test SCD1: simulate an airport city correction → verify all records updated

### Phase 6: Gold Layer
- [ ] Build `dim_date` (pre-generated)
- [ ] Build `dim_carrier` (from SCD2 airlines)
- [ ] Build `dim_airport` (from SCD1 airports)
- [ ] Build `dim_route`
- [ ] Build `fact_daily_flight_performance`
- [ ] Build `fact_weather_delay_correlation` (join flights + weather)
- [ ] Build `fact_monthly_carrier_kpi` (with fuel price enrichment)
- [ ] Build `fact_flight_features` (ML-ready denormalized)
- [ ] Verify every Gold table answers a specific business question

### Phase 7: Data Quality Framework
- [ ] Centralize data quality rules in configuration
- [ ] Implement quality check functions (nulls, duplicates, ranges, freshness, completeness)
- [ ] Record check results in `audit.data_quality_results`
- [ ] Implement threshold-based alerting (>5% reject rate → warning; >20% → failure)
- [ ] Test: intentionally corrupt data → verify detection and quarantine

### Phase 8: Incremental Processing & Late-Arriving Data
- [ ] Verify end-to-end incremental: first run (historical), second run (new month), third run (another month)
- [ ] Simulate late-arriving data: insert a record for a past date → verify it flows through correctly
- [ ] Verify Gold aggregates are corrected for affected dates
- [ ] Demonstrate idempotency: run the same batch twice → verify no corruption

### Phase 9: Airflow Orchestration
- [ ] Create main DAG: `skyops_daily_pipeline`
- [ ] Define tasks: check_source → ingest → upload → bronze → silver_transform → silver_quality → gold → validate → notify
- [ ] Implement task dependencies
- [ ] Configure retries (3 retries, 5 min delay)
- [ ] Implement failure callbacks (on_failure_callback)
- [ ] Implement success callbacks
- [ ] Add Airflow Variables/Connections for config
- [ ] Test `catchup=True` for backfill
- [ ] Test manual trigger with date parameters

### Phase 10: Monitoring & Alerting
- [ ] Build `audit.pipeline_runs` table
- [ ] Build `audit.file_tracking` table
- [ ] Build `audit.data_quality_results` table
- [ ] Implement record count tracking (received vs. processed vs. rejected)
- [ ] Implement duration tracking
- [ ] Implement freshness checks (when was the last successful run?)
- [ ] Create monitoring dashboard (can be a simple notebook or HTML report)
- [ ] Test: what does the monitoring show after a failure?

### Phase 11: Failure Testing
- [ ] Simulate API unavailable → verify retry + alert
- [ ] Simulate empty file → verify detection + skip
- [ ] Simulate corrupted file → verify quarantine
- [ ] Simulate duplicate file → verify idempotent handling
- [ ] Simulate schema change → verify schema evolution or controlled failure
- [ ] Simulate data quality failure → verify quarantine + alert
- [ ] Simulate Airflow task failure → verify retry + logging
- [ ] Simulate partial pipeline failure → verify recovery from checkpoint

### Phase 12: Optimization
- [ ] Implement OPTIMIZE on Delta tables
- [ ] Implement Z-ORDER or Liquid Clustering on Gold facts
- [ ] Demonstrate partition pruning with EXPLAIN
- [ ] Implement VACUUM for Delta table cleanup
- [ ] Compare query performance before/after optimization
- [ ] Profile Spark execution plans
- [ ] Identify and fix any small-file problems

### Phase 13: Testing
- [ ] Unit tests for transformation functions
- [ ] Data tests (schema, nulls, duplicates, business rules)
- [ ] Integration tests (source → bronze, bronze → silver, silver → gold)
- [ ] Pipeline tests (success, failure, retry, reprocessing, idempotency)
- [ ] End-to-end test: clean run from empty state to Gold

### Phase 14: Production Hardening
- [ ] Security review: no secrets in code, `.env` properly configured
- [ ] Documentation: README, data dictionary, runbook
- [ ] Cost review: verify everything stays within free tiers
- [ ] Storage cleanup scripts
- [ ] Disaster recovery: can we rebuild from source files?
- [ ] Final code review and refactoring

---

## PROJECT REPOSITORY STRUCTURE

```
skyops-aviation-analytics/
│
├── README.md                       # Project overview, setup, architecture
├── requirements.txt                # Python dependencies
├── .env.example                    # Template for environment variables
├── .gitignore                      # Ignore data/, .env, __pycache__, etc.
│
├── configs/                        # Configuration files
│   ├── pipeline_config.yaml        # Pipeline parameters (dates, paths, thresholds)
│   ├── data_quality_rules.yaml     # DQ rules per table
│   └── source_config.yaml          # API endpoints, URLs, credentials refs
│
├── ingestion/                      # Source data ingestion scripts
│   ├── __init__.py
│   ├── bts_downloader.py           # Download BTS CSV files
│   ├── weather_collector.py        # Fetch Open-Meteo weather data
│   ├── eia_collector.py            # Fetch EIA fuel prices
│   ├── lookup_downloader.py        # Download BTS lookup tables
│   └── utils.py                    # Common ingestion utilities (retry, logging)
│
├── schemas/                        # Explicit schema definitions
│   ├── __init__.py
│   ├── bronze_schemas.py           # StructType schemas for Bronze
│   ├── silver_schemas.py           # StructType schemas for Silver
│   └── gold_schemas.py             # Schema definitions for Gold tables
│
├── transformations/                # Bronze → Silver → Gold logic
│   ├── __init__.py
│   ├── bronze/
│   │   ├── __init__.py
│   │   ├── flights_bronze.py       # BTS CSV → Bronze Delta
│   │   ├── weather_bronze.py       # Weather JSON → Bronze Delta
│   │   ├── fuel_bronze.py          # EIA JSON → Bronze Delta
│   │   └── lookups_bronze.py       # Lookup CSVs → Bronze Delta
│   ├── silver/
│   │   ├── __init__.py
│   │   ├── flights_silver.py       # Bronze → Silver flights
│   │   ├── weather_silver.py       # Bronze → Silver weather (hourly→daily)
│   │   ├── fuel_silver.py          # Bronze → Silver fuel prices
│   │   ├── airlines_scd2.py        # SCD Type 2 for airlines
│   │   └── airports_scd1.py        # SCD Type 1 for airports
│   └── gold/
│       ├── __init__.py
│       ├── dim_date.py             # Date dimension generator
│       ├── dim_carrier.py          # Carrier dimension (from SCD2)
│       ├── dim_airport.py          # Airport dimension (from SCD1)
│       ├── dim_route.py            # Route dimension
│       ├── fact_daily_performance.py
│       ├── fact_weather_correlation.py
│       ├── fact_monthly_kpi.py
│       └── fact_flight_features.py
│
├── data_quality/                   # Data quality framework
│   ├── __init__.py
│   ├── quality_engine.py           # Core DQ check engine
│   ├── rules.py                    # Rule implementations
│   ├── quarantine.py               # Bad record handling
│   └── reporter.py                 # DQ results reporting
│
├── audit/                          # Audit and lineage
│   ├── __init__.py
│   ├── pipeline_tracker.py         # Pipeline run metadata
│   ├── file_tracker.py             # Source file tracking
│   └── models.py                   # Audit table schemas
│
├── airflow/                        # Airflow DAGs and config
│   ├── dags/
│   │   ├── skyops_daily_pipeline.py    # Main orchestration DAG
│   │   ├── skyops_backfill.py          # Backfill DAG
│   │   └── skyops_maintenance.py       # OPTIMIZE, VACUUM, cleanup
│   ├── plugins/
│   │   └── operators/
│   │       └── databricks_operators.py  # Custom operators if needed
│   └── config/
│       └── connections.json        # Airflow connection definitions
│
├── notebooks/                      # Databricks notebooks
│   ├── 01_explore_bts_data.py      # Data exploration
│   ├── 02_bronze_processing.py     # Bronze notebook
│   ├── 03_silver_processing.py     # Silver notebook
│   ├── 04_gold_processing.py       # Gold notebook
│   ├── 05_data_quality_review.py   # DQ results review
│   └── 06_query_examples.py        # Gold layer query examples
│
├── sql/                            # SQL scripts
│   ├── create_tables.sql           # DDL for Delta tables
│   ├── gold_queries.sql            # Reporting queries
│   └── audit_queries.sql           # Audit/monitoring queries
│
├── tests/                          # Test suite
│   ├── __init__.py
│   ├── unit/
│   │   ├── test_transformations.py
│   │   ├── test_data_quality.py
│   │   └── test_schema_validation.py
│   ├── integration/
│   │   ├── test_bronze_pipeline.py
│   │   ├── test_silver_pipeline.py
│   │   └── test_gold_pipeline.py
│   └── e2e/
│       └── test_full_pipeline.py
│
├── scripts/                        # Utility scripts
│   ├── setup_local_env.sh          # Local environment setup
│   ├── setup_s3.sh                 # AWS S3 bucket and IAM policy configuration
│   ├── generate_test_data.py       # Synthetic data generator
│   └── cleanup_storage.py          # EBS and S3 log cleanup utility
│
├── monitoring/                     # Monitoring and alerting
│   ├── dashboard.py                # Simple monitoring dashboard
│   ├── alerts.py                   # Alert configuration
│   └── health_check.py            # Pipeline health checks
│
├── docs/                           # Documentation
│   ├── architecture.md             # Detailed architecture doc
│   ├── data_dictionary.md          # Column-level documentation
│   ├── runbook.md                  # Operations runbook
│   ├── troubleshooting.md          # Common issues and fixes
│   └── decisions/                  # Architecture Decision Records
│       ├── ADR-001-cloud-choice.md
│       ├── ADR-002-partitioning.md
│       └── ADR-003-scd-strategy.md
│
└── data/                           # LOCAL ONLY — gitignored
    ├── landing/                    # Raw downloaded files
    │   ├── bts/
    │   ├── weather/
    │   ├── fuel/
    │   └── lookups/
    ├── bronze/                     # Local Bronze Delta tables
    ├── silver/                     # Local Silver Delta tables
    ├── gold/                       # Local Gold Delta tables
    └── quarantine/                 # Rejected records
```

---

> [!TIP]
> **Next Steps:** This document covers Steps 1-10 of your requested first response. When you're ready to proceed, we will begin **Phase 0 completion** (creating the Git repository structure, Python environment) and then move into **Phase 1** (environment setup). Each phase will include concept explanations, implementation, testing, failure scenarios, and exercises as outlined in your teaching style requirements.

---

> [!IMPORTANT]
> **Key Engineering Decision Acknowledged:** The Databricks Free Edition's restricted outbound internet access fundamentally shapes our architecture. API ingestion *must* happen outside Databricks. This is actually a blessing in disguise — it forces the same separation of concerns (ingestion vs. processing) that production data platforms use. In a real company, you would never have your Spark cluster making HTTP calls to external APIs. Ingestion is handled by dedicated services (Lambda, Cloud Functions, Airflow operators), and Spark processes data that's already landed in object storage.
