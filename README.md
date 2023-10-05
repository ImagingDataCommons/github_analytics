# GitHub and Google Analytics Data Collection

This repository contains Python code for collecting various GitHub analytics data from public repositories of Imaging Data Commons (IDC) and select repositories from Quantitative Image Informatics for Cancer Research (QIICR) GitHub organizations. Additionally, anonymous data captured using Google Analytics 4 is retrieved via the Google Analytics API. GitHub Actions automate the execution of Python scripts daily.

The collected data is stored in Google BigQuery and visualized in an Apache Superset instance, enabling us to gain insights into utilization and engagement with IDC.

## Table of Contents
- [GitHub Analytics](#github-analytics)
  - [Clone Traffic](#clone-traffic)
  - [Views Traffic](#views-traffic)
  - [Top Referrers](#top-referrers)
  - [Top Paths](#top-paths)
  - [Contributor Commit Activity](#contributor-commit-activity)
- [Google Analytics 4](#google-analytics-4)
  - [Audience Overview](#audience-overview)
  - [Acquisition Overview](#acquisition-overview)
  - [Behavior Overview](#behavior-overview)

## GitHub Analytics

### Clone Traffic
Collects clone and unique clone counts for specified repositories.

### Views Traffic
Gathers view and unique view counts for repositories.

### Top Referrers
Captures the top 10 referrers over the last 14 days.

### Top Paths
Records the top 10 paths over the last 14 days.

### Contributor Commit Activity
Tracks commits by contributors, including a Weekly Hash.

## Google Analytics 4

A Python script is used for collecting and analyzing data from Google Analytics 4 (GA4), including Audience Overview, Acquisition Overview, and Behavior Overview.

### Audience Overview
Retrieves and analyzes data related to user engagement on specified websites. Metrics include total users, new users, sessions, engaged sessions, screen page views, and average session duration.

### Acquisition Overview
Focuses on data related to user acquisition and traffic sources. Provides insights into how users are finding and accessing the websites.

### Behavior Overview
Analyzes user behavior on websites, including page views, session durations, and engagement.


