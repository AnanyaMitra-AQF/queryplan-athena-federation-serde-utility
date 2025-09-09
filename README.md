# Substrait Plan Generator for Athena Connectors
A Python utility to generate **Substrait plans** from table schemas for AWS Athena connectors. This tool allows developers to programmatically create Substrait `ReadRel` or `ProjectRel` plans with optional filter predicates and projection pushdowns.
---
## Table of Contents
* [Features](#features)
* [Prerequisites](#prerequisites)
* [Installation](#installation)
* [Usage](#usage)
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

## Configuration
Schema file format example(schema.sql):
```sql
CREATE TABLE fruit (
name VARCHAR,
color VARCHAR
);
```

---
## Example usage from terminal
Generate a plan as per your SQL query:
```bash
python generate_plan.py "SELECT * FROM fruit limit 20"
```
This will print the substrait plan generated into your terminal.

Note: Substrait expression for your query and schema will be available in _Isthmus_substrait.json_. 
