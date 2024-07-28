import os
import requests
import zipfile
import boto3
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

import time

session = boto3.Session(profile_name='default') 
s3_client = session.client('s3')

BUCKET_NAME = 'fiap-challenge-bovespa-data-raw'

def render_page(url):
    options = Options()
    options.headless = True 
    driver = webdriver.Firefox(options=options)
    driver.get(url)

    time.sleep(10)
    paragraphs = driver.find_elements(By.CSS_SELECTOR, 'p.primary-text')

    page_source = driver.page_source
    driver.quit()
    return page_source, paragraphs

def main():
    url = 'https://arquivos.b3.com.br/negocios'
    page_source, paragraphs = render_page(url)
    
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(page_source, 'html.parser')
    paragraphs = soup.select('p.primary-text')

    links = []
    for paragraph in paragraphs:
        a_tag = paragraph.find('a', href=True)
        if a_tag:
            link = a_tag['href']
            links.append(link)

    download_extract_and_upload_to_s3(links, 'data-extract-zip')

def download_extract_and_upload_to_s3(links, extract_to):
    if not os.path.exists(extract_to):
        os.makedirs(extract_to)

    for link in links:
        zip_file_path = os.path.join(extract_to, os.path.basename(link))
        
        with requests.get(link, stream=True) as r:
            r.raise_for_status()
            with open(zip_file_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

        # Extrair o arquivo .zip
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        
        # Upload dos arquivos extraídos para o S3
        for root, dirs, files in os.walk(extract_to):
            for file in files:
                file_path = os.path.join(root, file)
                s3_key = os.path.relpath(file_path, extract_to)
                upload_to_s3(file_path, s3_key)

def upload_to_s3(file_path, s3_key):
    try:
        s3_client.upload_file(file_path, BUCKET_NAME, s3_key)
        print(f'Successfully uploaded {file_path} to s3://{BUCKET_NAME}/{s3_key}')
    except NoCredentialsError:
        print('Credentials not available')
    except PartialCredentialsError:
        print('Incomplete credentials provided')
    except Exception as e:
        print(f'Failed to upload {file_path} to s3://{BUCKET_NAME}/{s3_key}: {e}')


if __name__ == '__main__':
    main()
