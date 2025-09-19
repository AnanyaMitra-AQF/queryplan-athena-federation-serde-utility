# Substrait Plan Generator for Athena Connectors
A Python utility to generate **Substrait plans** from table schemas for AWS Athena connectors. This tool allows developers to programmatically create Substrait `ReadRel` or `ProjectRel` plans with optional filter predicates and projection pushdowns.
---
## Table of Contents
* [Features](#features)
* [Prerequisites](#prerequisites)
* [Installation](#installation)
* [Usage](#usage)
    * [Substrait Plan Generator](#substrait-plan-generator)
    * [Block Parser](#block-parser)

---
## Features
* Generate Substrait plans from table schema and an SQL query for Athena connectors.
---
## Prerequisites
* Python 3.9+
---
## Installation
Clone the repository:
```bash
git clone git@github.com:AnanyaMitra-AQF/queryplan-athena-federation-serde-utility.git
```
Create a virtual environment (recommended):
```bash
python -m venv venv
source venv/bin/activate
```
Install dependencies:
```bash
pip install -r requirements.txt
```
---
## Usage
### Substrait Plan Generator
Generate plans from SQL queries against schemas.
**Schema file format example (`schema.sql`):**
```sql
CREATE TABLE fruit (
    name VARCHAR,
    color VARCHAR
);
```
**Run:**
```bash
python generate_plan.py "SELECT * FROM fruit WHERE color = 'red' limit 20"
```
This will print the Plan object and Substrait plan in your terminal.
---
### Block Parser
The **Block Parser utility** converts Athena `ReadRecordResponse Block` dumps into row-wise human-readable records and diffs.
**Input files :**
* `mainline_block.txt` → Mainline code Block dump
* `updated_block.txt` → Updated code Block dump
  **Outputs:**
* `mainline_records.txt` → Parsed row strings from mainline
* `updated_records.txt` → Parsed row strings from updated code
* `diff_records.txt` → Row-by-row differences
  **Run:**
```bash
python parse_block.py
```
**Example Block input:**
```
Block{rows=3, id=[1,2,3], name=[Alice,Bob,Charlie], active=[true,false,true]}
```
**Parsed output (`mainline_records.txt` and `updated_records.txt`):**
```
[id : 1], [name : Alice], [active : true]
[id : 2], [name : Bob], [active : false]
[id : 3], [name : Charlie], [active : true]
```
**Diff output (`diff_records.txt`):**
```
Row 1:
  Mainline: [id : 2], [name : Bob], [active : false]
  Updated : [id : 2], [name : Bobby], [active : true]
```
---
Outputs will be written to `mainline_records.txt`, `updated_records.txt`, and `diff_records.txt`.
