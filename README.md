# Linking Domestic Energy Performance Certificates (EPCs) and and Land Registry Price Paid Data (PPD) with UPRN


This project is licensed under the terms of the [Creative Commons Attribution-NonCommercial (CC-BY-NC)](https://creativecommons.org/licenses/by-nc/4.0/). 

This project shows the codex for OS short term project. This whole research is **not** allowed to be used  **commercially**. 

## 1. Getting Started

The address matching proceess are Domestic EPCs and Land Registry PPD ARE conducted in R, with data inputs and outputs stored in a PostGIS database. Figure 1 displays the whole workflow.

![](pic/f1.png)

Figure 1  The overall workflow of this project

### 1.1 Create a new spatial database in PostgreSQL
Create a new PostGIS database and named as **os_ubdc** (https://postgis.net/workshops/postgis-intro/creating_db.html). Here, the password of postgres is assumed to be **654321**.
### 1.2 Set the working directory for R
The process for setting the working directory is listed below:
- Create a directory named "OS_Data" on your D: drive.
- Create a sub-directory named "e90_ab_plus_csv_gb" in "OS_Data" folder.This e90_ab_plus_csv_gb folder stores OS AdddressBase Plus (AB_Plus_Data.csv).
- Create a sub-directory named "EPC" in "OS_Data" folder. This EPC folder stores whole list of Domestic EPC dataset.
- Create a sub-directory named "PPD" in "OS_Data" folder. This PPD folder stores Land Registry PPD(pp-complete.csv).