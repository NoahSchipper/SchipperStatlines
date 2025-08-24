# SchipperStatlines

An interactive baseball statistics project modeled after Stathead's baseball player and team comparison tools. This project provides comprehensive baseball analytics and comparison capabilities using historical and modern baseball data.

## Overview

SchipperStatlines aims to replicate and extend the functionality of popular baseball analytics platforms, offering users the ability to compare players, analyze team performance, and explore baseball statistics in an interactive format.

## Data Sources

This project leverages multiple high-quality baseball data sources:

1. Lahman Database: Historical baseball statistics dating back to 1871

2. pybaseball: Modern baseball data and advanced statistics

3. Jeff Bagwell Data: Specialized baseball analytics datasets

4. Retrosheet: Play-by-play baseball data for detailed analysis

## Features

1. Player Comparisons: Side-by-side statistical comparisons between baseball players

2. Team Analysis: Comprehensive team performance metrics and historical data

3. Interactive Interface: User-friendly tools for exploring baseball statistics

4. Historical Data: Access to extensive historical baseball records

5. Advanced Metrics: Modern sabermetric statistics and analytics

## Installation

git clone https://github.com/NoahSchipper/SchipperStatlines.git

cd SchipperStatlines

pip install -r requirements.txt

## Project Structure

├── procfile    # Necessary for Render Deployment

├── app.py # Main python file

├── baseball.db  # SQLite database

├── requirements.txt      # Project dependencies

├── render.yaml    # Necessary for Render deployment

└── README.md            # This file

## Data Attribution

This project uses data from:

The Lahman Baseball Database

pybaseball library

Jeff Bagwell's baseball data collections

Retrosheet organization

## Notes

This project is inspired by and modeled after stathead baseball's versus finder. Schipper Statlines is created for educational purposes

The code in app.py isn't the cleanest due to a time crunch before the fall semester starts. I will organize it if/when I find the time.

The code for the player version of Schipper Statlines is still present in app.py despite it not being interactable so that viewers here can still look at that code.

© Noah Schipper. All rights reserved.

