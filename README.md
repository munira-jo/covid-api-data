# covid-api-data

## Overview

This is a simple project that extacts data from this [COVID data API](https://covidtracking.com/data/api/version-2) (using the single day of data endpoint). The data is then to the tables `base_stats` and `outcome_stats` on a Postgres database. This is set up to run on a scheduled basis 
everyday at 22:00 SAST, querying the API for the data of the previous day.

## How to run locally

To run this project, make sure you have Docker and Docker Compose installed locally. 

A `.env` file containing the database credentials needs to inserted in the `ETL` directory, as well as the parent directory. This file is 
provided via email.

Then simply clone the repository and run `docker compose up --build`.
