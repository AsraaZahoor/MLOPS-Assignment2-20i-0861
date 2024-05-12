from airflow import DAG  # Imports the DAG class necessary for creating a DAG.
from datetime import datetime, timedelta  # Imports datetime utilities for handling date and time.
from airflow.operators.python import PythonOperator  # Importing PythonOperator to execute Python functions.
import requests  # Library to make HTTP requests.
from bs4 import BeautifulSoup  # Library for web scraping.
import csv  # Library to work with CSV files.
import re  # Library for regular expression operations.
import time  # Library to handle time-related tasks.
import os  # Library to interact with the operating system.

# ANSI escape sequences for coloring terminal output.
PURPLE = "\033[94m"
GREEN = "\033[92m"
RED = "\033[91m"
RESET = "\033[0m"

#-------------------------lifecycle functions-------------------------#

def extract_data(url):
    # Logs the start time, performs an HTTP request, and parses the content.
    start_time = time.strftime("%Y%m%d-%H%M%S")
    print(f"\nExtracting data from {url}")
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Scrapes links and article data from the webpage.
    links = [link.get('href') for link in soup.find_all('a', href=True)]
    articles = soup.find_all('article')
    article_data = []
    for idx, article in enumerate(articles):
        title = article.find('h2').text.strip() if article.find('h2') else None
        description = article.find('p').text.strip() if article.find('p') else None
        article_data.append({'id': idx+1, 'title': title, 'description': description, 'source': url})

    # Logs the time taken for extraction and the amount of data extracted.
    end_time = time.strftime("%Y%m%d-%H%M%S")
    print(f"Extracted data in {calculate_duration(start_time, end_time)} seconds")
    print(f"Extracted {len(article_data)} articles and {len(links)} links from {url}")
    return links, article_data

def save_to_csv(file_name, articles):
    # Writes data to a CSV file.
    with open(file_name, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['id', 'title', 'description', 'source']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for article in articles:
            writer.writerow(article)

def preprocess(text):
    # Cleans text by removing HTML tags, non-alphabetic characters, and extra spaces.
    clean_text = re.sub('<.*?>', '', text)
    clean_text = re.sub('[^a-zA-Z]', ' ', clean_text)
    clean_text = clean_text.lower()
    clean_text = re.sub(' +', ' ', clean_text)
    return clean_text

def clean_data(data):
    # Applies preprocessing to the title and description of each article.
    cleaned_data = []
    for article in data:
        article['title'] = preprocess(article['title']) if article.get('title') else None
        article['description'] = preprocess(article['description']) if article.get('description') else None
        cleaned_data.append(article)
    return cleaned_data

def calculate_duration(start_time, end_time):
    # Calculates the duration between two timestamps.
    start = time.strptime(start_time, "%Y%m%d-%H%M%S")
    end = time.strptime(end_time, "%Y%m%d-%H%M%S")
    duration = time.mktime(end) - time.mktime(start)
    return duration



#-------------------------airflow task specific functions-------------------------#

def git_push():
    # Executes a series of git commands to update the repository.
    os.system('git status')
    os.system('git pull')
    os.system('git status')
    os.system('git add .')
    os.system('git status')
    os.system('git commit -m "updated automatically by dvc"')
    os.system('git status')
    os.system('git push origin main')
    os.system('git status')

def dvc_push():
    # Adds files to DVC and pushes to the remote storage.
    os.system('dvc add data/extracted.csv')
    os.system('dvc push')


# URL list and file path setup.
urls = ['https://www.dawn.com/', 'https://www.bbc.com/']
filename = "C:/Users/HP/MLOPS_Assignment2_20i-0861/data/extracted.csv"

# Airflow default arguments and DAG definition.
default_args = {
    'owner': 'Asraa',
}

dag = DAG(
    dag_id='Dag_1',
    default_args=default_args,
    description='dag for assignment 2',
    tags=['assignment2', 'mlops'],
    catchup=False,
    schedule=None,
    start_date=datetime(2024, 5, 9),
)

# Task definitions using PythonOperator for executing Python functions.
with dag:
    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract_data,
        op_kwargs={'urls': urls},
        provide_context=True
    )

    preprocess_task = PythonOperator(
        task_id='preprocess_task',
        python_callable=preprocess,
        op_kwargs={'data': extract_task.output},
        provide_context=True
    )

    save_task = PythonOperator(
        task_id='save_task',
        python_callable=save_to_csv,
        op_kwargs={'file_name': filename, 'data': preprocess_task.output},
        provide_context=True
    )

    dvc_push_task = PythonOperator(
        task_id='dvc_push_task',
        python_callable=dvc_push,
    )

    git_push_task = PythonOperator(
        task_id='git_push_task',
        python_callable=git_push,
    )

# Task execution sequence.
extract_task >> preprocess_task >> save_task >> dvc_push_task >> git_push_task


### use to run the pipeline manually
def main():
    dawn_url = 'https://www.dawn.com/'
    bbc_url = 'https://www.bbc.com/'
    file_name = "C:/Users/HP\MLOPS_Assignment2_20i-0861/data/extracted.csv"

    dawn_links, dawn_articles = extract_data(dawn_url)
    dawn_data = clean_data(dawn_articles, dawn_url)

    bbc_links, bbc_articles = extract_data(bbc_url)
    bbc_data = clean_data(bbc_articles, bbc_url)

    data = dawn_data + bbc_data
    save_to_csv(file_name, data)

    print(f"Data saved to{GREEN} {file_name} {RESET}\n")

if __name__ == '__main__':
    main()