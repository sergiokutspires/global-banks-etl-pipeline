# Global Banks ETL Pipeline

An end-to-end ETL (Extract, Transform, Load) pipeline built in Python to collect, process, and store market capitalization data of the world's largest banks.

---

## 📊 Project Overview

This project performs the following steps:

### 1️⃣ Extract
- Scrapes the top 10 largest banks by market capitalization.
- Loads raw data into a Pandas DataFrame.

### 2️⃣ Transform
- Cleans the Market Cap column.
- Converts USD values to:
  - GBP
  - EUR
  - INR
- Uses exchange rates provided in `exchange_rate.csv`.

### 3️⃣ Load
- Saves processed data into:
  - A CSV file
  - A SQLite database table.
- Executes SQL queries for validation.
- Logs ETL process steps into a log file.

---

## 🛠 Technologies Used

- Python
- Pandas
- NumPy
- BeautifulSoup (bs4)
- Requests
- SQLite3
- lxml

---

## 📂 Project Structure

```
global-banks-etl-pipeline/
│
├── banks_project.py
├── exchange_rate.csv
├── requirements.txt
├── .gitignore
└── README.md
```

---

## ▶️ How to Run

1. Clone the repository:

```bash
git clone https://github.com/sergiokutspires/global-banks-etl-pipeline.git
cd global-banks-etl-pipeline
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Run the pipeline:

```bash
python banks_project.py
```

---

## 🧠 Example SQL Queries Executed

```sql
SELECT * FROM Largest_banks;

SELECT AVG(MC_GBP_Billion) FROM Largest_banks;

SELECT Name FROM Largest_banks LIMIT 5;
```

---

## 📚 Context

This project was developed as part of the IBM Data Engineering Professional Certificate program on Coursera and further refined for portfolio presentation.

---

## 🚀 Future Improvements

- Modularize pipeline into separate files (extract.py, transform.py, load.py)
- Add Docker support
- Add data validation checks
- Integrate Airflow for orchestration