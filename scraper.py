import os
import requests
import zipfile
import boto3
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

import time
import datetime

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

    download_and_upload_to_s3(links)

def download_and_upload_to_s3(links):
    for link in links:
        zip_file_name = os.path.basename(link)
        zip_file_name_with_extension = zip_file_name + ".zip"

        date_part = zip_file_name_with_extension.split(".")[0]
        date_object = datetime.datetime.strptime(date_part, "%Y-%m-%d")
        formatted_date = date_object.strftime("%Y%m%d")

        s3_key = f"data-zip-{formatted_date}.zip" 
        
        with requests.get(link, stream=True) as r:
            r.raise_for_status()
            
            try:
                s3_client.upload_fileobj(r.raw, BUCKET_NAME, s3_key)
                print(f'Successfully uploaded {s3_key} to s3://{BUCKET_NAME}/{s3_key}')
            except NoCredentialsError:
                print('Credentials not available')
            except PartialCredentialsError:
                print('Incomplete credentials provided')
            except Exception as e:
                print(f'Failed to upload {s3_key} to s3://{BUCKET_NAME}/{s3_key}: {e}')

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