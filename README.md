# Linking Land Registry Price Paid Data (PPD) and Domestic Energy Performance Certificates (EPCs) with UPRN


This project is licensed under the terms of the [Creative Commons Attribution-NonCommercial (CC-BY-NC)](https://creativecommons.org/licenses/by-nc/4.0/). 

This project shows the code for OS short term project.This research is **not** allowed to be used  **commercially**. 

## 1. Getting Started

All matching rules were written in R with data inputs and outputs stored in a PostGIS database. Figure 1 displays the whole work flowchart.

### 1.1 Create a new spatial database in PostgreSQL
Create a new PostGIS database and named as **osubdc** (https://postgis.net/workshops/postgis-intro/creating_db.html). Here, the password of postgres user is assumed to be **654321**.
### 1.2 Set the working directory for R
The process for setting the working directory is listed below:
- Create a directory named "OS_Data" on your D: drive.

- Create a sub-directory named "e90_ab_plus_csv_gb" in "OS_Data" folder.  

