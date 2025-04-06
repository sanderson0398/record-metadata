# Family History Library Catalog Metadata Clean-up

This project involves analyzing and cleaning metadata from the Family History Library Catalog. The goal is to improve data quality, identify inconsistencies, and provide actionable insights to enhance catalog usability.

## Project Overview

### Purpose
The Family History Library Catalog contains metadata with varied formats and structures. This project identifies and addresses inconsistencies, including missing fields, improper formatting, and redundant data.

### Objectives
1. **Identify Issues:** Analyze metadata to uncover missing, inconsistent, or incorrect values.
2. **Clean and Standardize:** Apply rules to format, standardize, and enrich metadata fields.
3. **Generate Insights:** Use visualizations and summaries to showcase trends and results of the clean-up process.

## Methodology

### Steps
1. **Data Extraction:** Metadata was imported from various files in `.xlsx` and `.csv` formats.
2. **Data Exploration:** Key fields such as language, publication location, publisher name, and publication date were examined for inconsistencies.
3. **Data Cleaning:** 
   - Removed special characters and unnecessary formatting.
   - Standardized naming conventions and date formats.
   - Identified and addressed missing or null values.
4. **Visualization and Analysis:** Used tools like Seaborn and Matplotlib for trend analysis and reporting.

### Tools and Technologies
- **Python Libraries:**
  - Polars for data manipulation.
  - Pandas for processing and cleaning.
  - Matplotlib and Seaborn for data visualization.
- **File Formats:**
  - `.xlsx` for raw metadata files.
  - `.csv` for combined and processed datasets.

## Results

- Cleaned metadata with consistent formats and reduced redundancy.
- Visualizations highlight key patterns and areas needing attention.
- Enhanced metadata quality for better usability in the catalog.
- 
### Presentation
From Winter 2025 semester
https://docs.google.com/presentation/d/1y2mI8iDXViq2o36hwFZ7qixGeR4Q26u22JaUb4CSVSI/edit?usp=sharing

### New code
https://colab.research.google.com/drive/1TUJ1lhxG01W8ffsdp8HpTDw8dQNPgCbw?usp=sharing

https://colab.research.google.com/drive/1EtH-qNKpro5JEq8zHAW67HOBY0y7S_lc?usp=sharing

https://colab.research.google.com/drive/1C1xSb0Q4hvW_D4GZfjC6xKPZaTQvdSlv?usp=sharing

---

### How to Use
1. Load the cleaned metadata file for analysis or integration.
2. Use the included scripts to further refine or analyze new datasets.
