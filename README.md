# BIG-DATA-ANALYSIS

*COMPANY*: CODTECH IT SOLUTIONS

*NAME*: RUQSAR FIRDOUS

*INTERN ID*: CT04DG2858

*DOMAIN*: DATA ANALYST

*DURATION* : 4 WEEKS

*MENTOR*: NEELA SANTOSH

# ✈️ Big Data Analysis of US Flight Delays using PySpark

## 📚 Project Overview

This repository contains my **Big Data analysis project** done as part of my internship. The primary goal of this project was to demonstrate my ability to handle large, real-world datasets using **PySpark**, perform meaningful data cleaning and transformation, and generate insights that help understand trends in flight delays and cancellations in the United States.

The dataset used is the **2015 US Flight Delays and Cancellations**, which includes millions of records about domestic flights in the US, airline information, and airport details. By analyzing this data, we can identify which airlines have the highest delays, which airports are the busiest, and which cities experience the most frequent flight disruptions.

---

## ⚙️ Tools & Editor

- **Big Data Tool:** PySpark (Apache Spark with Python API)
- **Editor:** Google Colab (cloud-based Jupyter Notebook)
- **Language:** Python 3

I chose Google Colab as my editor because it is beginner-friendly, does not require local installation of Spark, and provides free cloud compute for running PySpark code interactively.

---

## 🗂️ Dataset

**Files used:**
- `flights.csv`: Detailed flight information (airline code, airports, delays).
- `airlines.csv`: Maps airline `IATA_CODE` to the airline's full name.
- `airports.csv`: Maps airport `IATA_CODE` to its city, state, and coordinates.

**Source:** [2015 Flight Delays and Cancellations — Kaggle](https://www.kaggle.com/datasets/usdot/flight-delays)

---

## ✅ What I Did

1. **Data Loading:**  
   Loaded the three CSV files into PySpark DataFrames and verified that schemas were correctly inferred.

2. **Joining Data:**  
   - **First Join:** Flights were joined with Airlines on `AIRLINE` code to map codes to full airline names.
   - **Second Join:** The result was then joined with Airports on `ORIGIN_AIRPORT` to get the city and state of the departure airport.
   This step demonstrated my understanding of how to merge related large datasets using joins.

3. **Data Cleaning:**  
   After joining, some rows contained `null` values due to unmatched codes. To handle this:
   - I filled missing airline names with `"Unknown Airline"`.
   - I filled missing city and state with `"Unknown City"` and `"Unknown State"`.
   - For numeric columns like `DEPARTURE_DELAY` and `ARRIVAL_DELAY`, I filled missing values with `0`.

4. **Data Analysis:**  
   Using PySpark’s `groupBy` and `agg` functions, I performed several analyses:
   - **Busiest Cities:** Counted total flights per city to identify the most active departure locations.
   - **Most Active Airlines:** Counted total flights per airline.
   - **Average Delays:** Computed average departure and arrival delays per city and per airline.
   - **Big Delays:** Filtered flights delayed more than 60 minutes to understand significant disruptions.
   - **Combined Grouping:** Found average delays by both airline and city to identify which airline had the most issues at which airports.

5. **Saving Results:**  
   As a final step, I exported one of the grouped DataFrames (average departure delay per airline) as a CSV file, demonstrating how to write PySpark results back to storage for reporting or dashboarding.

---

## 📈 Key Learnings

This internship task helped me strengthen my skills in:
- Working with **big datasets** that cannot be processed using standard pandas.
- Performing **multi-step joins** in PySpark to enrich raw data with related information.
- Handling missing data using `.fillna()` to ensure analysis results are reliable.
- Using **Google Colab** as an easy, practical environment for running distributed PySpark jobs.
- Writing clean, readable, and reusable PySpark code.

---

## 🔑 Why This Matters

Delays and cancellations in the aviation industry affect millions of passengers every year. Analyzing such data using Big Data tools can help stakeholders like airlines, airport authorities, and passengers understand trends and make data-driven decisions to improve operations and customer satisfaction.

This project demonstrates my ability to:
- Use **Big Data frameworks** like PySpark in a practical, real-world context.
- Clean and prepare large data pipelines.
- Generate and share meaningful business insights.
- Document and share my work using version control tools like **GitHub**.

---

## 📎 How to Run

This project was written and tested entirely in **Google Colab**.  
To run this yourself:
1. Upload the dataset files (`flights.csv`, `airlines.csv`, `airports.csv`) to your Colab session.
2. Install PySpark (`!pip install pyspark`).
3. Run the notebook cells step-by-step.

---

## 🏷️ License

This repository is for educational purposes only.

---

## 🚀 Author

**RUQSAR FIRDOUS**  
Internship Project — Big Data Analysis with PySpark  
[GitHub Profile](https://github.com/ruqsarfirdous)

---

*Feel free to fork, clone, or adapt for your own learning!*
