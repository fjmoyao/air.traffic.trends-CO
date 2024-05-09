# Colombian Air traffic
Analysis of the Colombian airline traffic, aiding in understanding trends and optimizing operations within the aviation sector.

## Description
This project involves scraping, cleaning, and analyzing a decade of airline traffic data from Colombia's Special Administrative Unit of Civil Aeronautics [Aerocivil](https://www.aerocivil.gov.co/). By using advanced data analytics tools and cloud-based platforms, this project uncovers significant insights into market dynamics, operational trends, and the overall health of the aviation sector in Colombia. For more detail visit the [Medium](https://medium.com/@frankj.mortiz/soaring-in-data-exploring-the-airline-market-in-colombia-7068fd0f255f) article.

## Requirements
- polars
- psycopg2
- BeautifulSoup
- requests

## Installation 
Get started with just a few commands! You can use either `pdm` or `pip` to install the necessary dependencies.

To install using pip, run:

```bash
pip install -r requirements.txt
```

## Usage

To download and preprocess the data, execute the preprocessing pipeline:

```bash
python preprocessing_pipeline.py
```

To create the database, run:

```bash
python create_DB.py
```

**Note:** You need to set your DB credentials in a *.env* file specifying db_name, db_user, db_password, db_host, db_port. The connector used is for PostgreSQL. However, you can modify it in the utils.py file if you want to connect to another type of database.

After running both scripts, you should end with the processed data on your disk inside *'data/silver_data'* and also as a table in your database. From there, you can perform your analysis on your platform of preference.

## Visualization and Analysis

The visualizations and data exploration was done using [Sigma](https://help.sigmacomputing.com/) a web-based data exploration and data visualization tool. Explore the visualizations and further analysis by accessing this [interactive dashboard](https://app.sigmacomputing.com/embed/1-4vxWhoJyQAEZrnypLXpIul?:nodeId=x5IFw14Sfe), or dive into the data with your preferred data analysis tools.

![Dashboard preview](images\dashboard_sigma.png)
