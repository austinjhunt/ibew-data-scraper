# Upwork Job - Build simple Python script to query specific website’s API directly, with Excel output

## Client: Michael Winchell

## Start Date: September 17, 2024

## Est. Time: 2-3 hrs.

### Detailed Client Requirements

Hi there:

There are two websites this will sift through. First is a union directory search tool (URL: ibew.org/Tools/Local-Union-Directory) that allows, on the front end, for users to search by local (which is a number), by VP District, and by State/Province. First, this needs to hit the API (which I’ve already done, but not on an iterative level), by querying by the following states: NY, CT, RI, MA, VT, NH, and ME. When you do, you’ll get a JSON response for local union ID and city / state. I need these 3 fields likely added to a data frame, and then I need it to do the search by Local, and, per local union ID found from the first pull, first pull in the classifications from the output, and then clicking on show county information, and then showing the counties listed, the population, the sq miles, percent, and jurisdiction per county.

Next, the script, after appending that data, will go to the website (Unionfacts.com/locals/International_Brotherhood_of_Electrical_Workers) containing the number of members per local union ID. It will search for the Union, and output the number of members. After consolidating all of that into a single dataframe, it’ll provide it as an excel output.

As I am very pressed for time and don’t have time to code it right now, please let me know how soon you can get this to me, and the number of hours you intend to take to work on it.

Best,
Michael

## Solution: IBEW Data Scraper

### Overview

This Python script ([main.py](./main.py)) scrapes and merges data about local unions from the **IBEW (International Brotherhood of Electrical Workers)** directory and the **UnionFacts** website. The data is retrieved using a combination of API requests and web scraping (where an API is not available) then processes and saves the data as an Excel file.

### Features:

- Queries IBEW API by state to retrieve local union details.
- Scrapes the UnionFacts website for additional union data (API not available, so I'm using BeautifulSoup). The table on this site has data for 800+ unions, so we filter out only those with a Local identifier.
- Enhances union data by adding classifications and county information using multithreading for efficiency.
- Cleans and flattens nested JSON structures (like `Counties`) for easier data processing.
- Outputs the result as an Excel file.

## Installation

### Requirements:

Make sure you have Python installed (>= 3.6) and install the required dependencies listed in the `requirements.txt`:

```bash
pip install -r requirements.txt
```

### Script Arguments

```bash

(venv) austinhunt@Austins-MBP-2 unionfacts % python main.py -h
usage: main.py [-h] --states STATES [--output OUTPUT]

IBEW Data Scraper

options:
  -h, --help       show this help message and exit
  --states STATES  Comma-separated list of state abbreviations to query, e.g. NY,CT,RI
  --output OUTPUT  Output file name (must end with .xlsx)
```

### Example:

```bash
(venv) austinhunt@Austins-MBP-2 unionfacts % python main.py --states=NY,CT,RI,MA,VT,NH,ME --output merged_union_data.xlsx

```
