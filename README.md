Elasticsearch Malicious Text Analyzer

This project is a monolithic Python application that analyzes text data for antisemitism and weapon mentions, using Elasticsearch as the main data store.

Overview

The app performs the following steps:

Reads a CSV file and converts it to a dictionary.

Opens a connection to Elasticsearch.

Indexes all documents into Elasticsearch.

Adds two additional fields to each document:

sentiment: the sentiment of the text (positive, negative, neutral).

weapons: a list of all weapons mentioned in the text.

Deletes documents that:

Are not antisemitic, and

Do not contain any weapons, and

Are not negative in sentiment.

All data is saved and managed in Elasticsearch for easy querying and analysis.

Features

Retrieve all documents that are antisemitic and contain at least one weapon.

Retrieve all documents that contain two or more weapons.

Usage

Place your CSV file with tweets in the data folder.

Set the environment variable CSV_PATH to point to your CSV file.

Run the application:

python system_manager/manager.py


Use the provided methods to query documents from Elasticsearch.



file system

ElasticSearch_Malicious_Text/
├── .gitignore
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── csv_to_dict/
│   └── load_csv.py
├── data/
│   ├── tweets_injected.csv
│   └── weapon_list.txt
├── elastic/
│   ├── elastic_base.py
│   └── elastic_manger.py
├── endpoints/
│   └── app.py
├── process/
│   ├── detect_weapon.py
│   └── sentiment.py
└── system_manager/
    └── manager.py
