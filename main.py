import os
import sys
import time
import shutil
import zipfile
import logging
import bu_alerts
import bu_alerts
import bu_config
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from datetime import date, datetime ,timedelta
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.support import expected_conditions as EC


def firefoxDriverLoader():
    try:
        mime_types = ['application/pdf', 'text/plain', 'application/vnd.ms-excel', 'test/csv', 'application/zip', 'application/csv',
                    'text/comma-separated-values', 'application/download', 'application/octet-stream', 'binary/octet-stream', 'application/binary', 'application/x-unknown']
        profile = webdriver.FirefoxProfile()
        profile.set_preference('browser.download.folderList', 2)
        profile.set_preference(
            'browser.download.manager.showWhenStarting', False)
        profile.set_preference('browser.download.dir', download_path)
        profile.set_preference('pdfjs.disabled', True)
        profile.set_preference(
            'browser.helperApps.neverAsk.saveToDisk', ','.join(mime_types))
        profile.set_preference(
            'browser.helperApps.neverAsk.openFile', ','.join(mime_types))
        driver = webdriver.Firefox(executable_path=GeckoDriverManager().install(),  firefox_profile=profile)
        return driver
    except Exception as e:
        logging.error(
            'Exception caught during execution firefoxDriverLoader() : {}'.format(str(e)))
        print('Exception caught during execution firefoxDriverLoader() : {}'.format(str(e)))
        raise e


def login(driver):
    try:
        driver.get(url_1)
        WebDriverWait(driver, 90).until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "#LoginUserId"))).send_keys(username)
        time.sleep(1)
        WebDriverWait(driver, 90).until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "#LoginPassword"))).send_keys(password)
        time.sleep(1)
        WebDriverWait(driver, 90).until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, ".node > form:nth-child(4) > input:nth-child(6)"))).click()
    except Exception as e:
        logging.error(
            'Exception caught during execution Login() : {}'.format(str(e)))
        print('Exception caught during execution Login() : {}'.format(str(e)))
        raise e


def get_data(driver):
    try:
        WebDriverWait(driver, 90).until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "div.mycdx-row:nth-child(2) > div:nth-child(3) > a:nth-child(1)"))).click()
        time.sleep(1)
        WebDriverWait(driver, 90).until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "#BIOURJA\ TRADING\ LLC"))).click()
        time.sleep(1)
    except Exception as e:
        logging.error(
            'Exception caught during execution get_data() : {}'.format(str(e)))
        print('Exception caught during execution get_data() : {}'.format(str(e)))
        raise e


def file_extraction(time_stamp, zipname, destination_path):
    try:
        zip_file = download_path + zipname
        extract_dir = download_path
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        for filename in os.listdir(extract_dir):
            if filename.endswith('.csv') and not filename.endswith("AM.csv") and not filename.endswith("PM.csv"):
                filename_without_csv = filename.split('.csv')[0]
                old_filename = os.path.join(extract_dir, filename)
                file = os.path.join(
                    extract_dir, filename_without_csv + '_' + time_stamp + '.xlsx')
                df = pd.read_csv(old_filename)
                os.remove(old_filename)
                df.to_excel(file, index=False)
                try:
                    shutil.copy(file, destination_path)
                except FileNotFoundError:
                    os.makedirs(destination_path, exist_ok=True)
                    shutil.copy(file, destination_path)
                fi = os.path.basename(file)
        os.remove(zip_file)
        os.remove(file)
        if fi.startswith(('PendingTrades', 'CompletedTrades')) and fi.endswith('.xlsx'):
            file_path = os.path.join(destination_path, fi)
            excel_data = pd.read_excel(file_path)
            excel_files.append(excel_data)
    except Exception as e:
        logging.error(
            'Exception caught during execution file_extraction() : {}'.format(str(e)))
        print('Exception caught during execution file_extraction() : {}'.format(str(e)))
        raise e


def loc_change_for_zip(time_stamp, zipname, destination_path):
    try:
        for filename in os.listdir(download_path):
            filename_without_zip = filename.split('.zip')[0]
            old_zipfile_name = download_path + filename
            new_name = os.path.join(
                download_path, filename_without_zip + '_' + time_stamp+'.zip')
            os.rename(old_zipfile_name, new_name)
            try:
                shutil.copy(new_name, destination_path)
            except FileNotFoundError:
                os.makedirs(destination_path, exist_ok=True)
                shutil.copy(new_name, destination_path)
        os.remove(new_name)
    except Exception as e:
        logging.error(
            'Exception caught during execution loc_change_for_zip() : {}'.format(str(e)))
        print('Exception caught during execution loc_change_for_zip() : {}'.format(str(e)))
        raise e


def download_file_pending_trades(driver, destination_path):
    try:
        driver.get(download_file_pending_trades_url)
        WebDriverWait(driver, 90).until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "tr.odd:nth-child(1) > td:nth-child(3) > form:nth-child(1) > input:nth-child(4)"))).click()
        time.sleep(1)
        soup = BeautifulSoup(driver.page_source, 'lxml')
        table = soup.find(lambda tag: tag.name == 'table')
        rows = table.findAll(lambda tag: tag.name == 'tr')
        table_row = rows[1].findAll(lambda tag: tag.name == 'td')
        li = table_row[1].text.split(" ")
        time_stamp = li[1]+li[2]
        time_stamp = time_stamp.replace(":", ".")
        zipname = "Pending Trades.zip"
        destination_path = destination_path + "PendingTrades\\" + \
            str(current_year) + "\\" + current_month + "\\" + "Test"
        file_extraction(time_stamp, zipname, destination_path)
        WebDriverWait(driver, 90).until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "tr.odd:nth-child(1) > td:nth-child(3) > form:nth-child(1) > input:nth-child(4)"))).click()
        time_stamp = li[0]+'_'+li[1]+li[2]
        time_stamp = time_stamp.replace(":", ".")
        time_stamp = time_stamp.replace("/", ".")
        loc_change_for_zip(time_stamp, zipname, destination_path)
        WebDriverWait(driver, 90).until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "tr.even:nth-child(2) > td:nth-child(3) > form:nth-child(1) > input:nth-child(4)"))).click()
        time.sleep(1)
        table_row = rows[2].findAll(lambda tag: tag.name == 'td')
        li = table_row[1].text.split(" ")
        time_stamp = li[0]+'_'+li[1]+li[2]
        time_stamp = time_stamp.replace(":", ".")
        time_stamp = time_stamp.replace("/", ".")
        loc_change_for_zip(time_stamp, zipname, destination_path)
        WebDriverWait(driver, 90).until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "tr.odd:nth-child(3) > td:nth-child(3) > form:nth-child(1) > input:nth-child(4)"))).click()
        time.sleep(1)
        table_row = rows[3].findAll(lambda tag: tag.name == 'td')
        li = table_row[1].text.split(" ")
        time_stamp = li[0]+'_'+li[1]+li[2]
        time_stamp = time_stamp.replace(":", ".")
        time_stamp = time_stamp.replace("/", ".")
        loc_change_for_zip(time_stamp, zipname, destination_path)
    except Exception as e:
        logging.error('Exception caught during execution download_file_pending_trades() : {}'.format(str(e)))
        print('Exception caught during execution download_file_pending_trades() : {}'.format(str(e)))
        raise e


def download_file_pending_trades_details(driver, destination_path):
    try:
        driver.get(download_file_pending_trades_details_url)
        WebDriverWait(driver, 90).until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "tr.odd:nth-child(1) > td:nth-child(3) > form:nth-child(1) > input:nth-child(4)"))).click()
        time.sleep(1)
        soup = BeautifulSoup(driver.page_source, 'lxml')
        table = soup.find(lambda tag: tag.name == 'table')
        rows = table.findAll(lambda tag: tag.name == 'tr')
        table_row = rows[1].findAll(lambda tag: tag.name == 'td')
        li = table_row[1].text.split(" ")
        time_stamp = li[1] + li[2]
        time_stamp = time_stamp.replace(":", ".")
        time_stamp = time_stamp.replace("/", ".")
        zipname = "Pending Trade Details.zip"
        destination_path = destination_path + "Pending Trade Details\\" + \
            str(current_year) + "\\" + current_month + "\\" + "Test"
        file_extraction(time_stamp, zipname, destination_path)
    except Exception as e:
        logging.error('Exception caught during download_file_pending_trades_details() : {}'.format(str(e)))
        print('Exception caught during download_file_pending_trades_details() : {}'.format(str(e)))
        raise e


def download_file_RIN_holdings(driver, destination_path):
    try:
        driver.get(download_file_RIN_holdings_url)
        WebDriverWait(driver, 90).until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "tr.odd:nth-child(1) > td:nth-child(3) > form:nth-child(1) > input:nth-child(4)"))).click()
        time.sleep(1)
        soup = BeautifulSoup(driver.page_source, 'lxml')
        table = soup.find(lambda tag: tag.name == 'table')
        rows = table.findAll(lambda tag: tag.name == 'tr')
        table_row = rows[1].findAll(lambda tag: tag.name == 'td')
        li = table_row[1].text.split(" ")
        time_stamp = li[1]+li[2]
        time_stamp = time_stamp.replace(":", ".")
        zipname = "RIN Holdings.zip"
        destination_path = destination_path + "RINHoldings\\" + \
            str(current_year) + "\\" + current_month + "\\" + "Test"
        file_extraction(time_stamp, zipname, destination_path)
    except Exception as e:
        logging.error(
            'Exception caught during execution download_file_RIN_Holdings() : {}'.format(str(e)))
        print('Exception caught during execution download_file_RIN_Holdings() : {}'.format(str(e)))
        raise e


def download_file_completed_trades(driver, destination_path):
    try:
        driver.get(download_file_completed_trades_url)
        WebDriverWait(driver, 90).until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "tr.odd:nth-child(1) > td:nth-child(3) > form:nth-child(1) > input:nth-child(4)"))).click()
        time.sleep(1)
        soup = BeautifulSoup(driver.page_source, 'lxml')
        table = soup.find(lambda tag: tag.name == 'table')
        rows = table.findAll(lambda tag: tag.name == 'tr')
        table_row = rows[1].findAll(lambda tag: tag.name == 'td')
        li = table_row[1].text.split(" ")
        time_stamp = li[1]+li[2]
        time_stamp = time_stamp.replace(":", ".")
        zipname = "Completed Trades.zip"
        destination_path = destination_path + "Completed Trades\\" + \
            str(current_year) + "\\" + current_month + "\\" + "Test"
        file_extraction(time_stamp, zipname, destination_path)
        WebDriverWait(driver, 90).until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "tr.odd:nth-child(1) > td:nth-child(3) > form:nth-child(1) > input:nth-child(4)"))).click()
        time_stamp = li[0]+'_'+li[1]+li[2]
        time_stamp = time_stamp.replace(":", ".")
        time_stamp = time_stamp.replace("/", ".")
        loc_change_for_zip(time_stamp, zipname, destination_path)
        WebDriverWait(driver, 90).until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "tr.even:nth-child(2) > td:nth-child(3) > form:nth-child(1) > input:nth-child(4)"))).click()
        time.sleep(1)
        table_row = rows[2].findAll(lambda tag: tag.name == 'td')
        li = table_row[1].text.split(" ")
        time_stamp = li[0]+'_'+li[1]+li[2]
        time_stamp = time_stamp.replace(":", ".")
        time_stamp = time_stamp.replace("/", ".")
        loc_change_for_zip(time_stamp, zipname, destination_path)
        WebDriverWait(driver, 90).until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "tr.odd:nth-child(3) > td:nth-child(3) > form:nth-child(1) > input:nth-child(4)"))).click()
        time.sleep(1)
        table_row = rows[3].findAll(lambda tag: tag.name == 'td')
        li = table_row[1].text.split(" ")
        time_stamp = li[0]+'_'+li[1]+li[2]
        time_stamp = time_stamp.replace(":", ".")
        time_stamp = time_stamp.replace("/", ".")
        loc_change_for_zip(time_stamp, zipname, destination_path)
    except Exception as e:
        logging.error(
            'Exception caught during download_file_completed_trades() : {}'.format(str(e)))
        print(
            'Exception caught during download_file_completed_trades() : {}'.format(str(e)))
        raise e


def download_file_transaction_status(driver, destination_path):
    try:
        driver.get(download_file_transaction_status_url)
        WebDriverWait(driver, 90).until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "tr.odd:nth-child(1) > td:nth-child(3) > form:nth-child(1) > input:nth-child(4)"))).click()
        logging.info("before time.sleep")
        time.sleep(1)
        soup = BeautifulSoup(driver.page_source, 'lxml')
        table = soup.find(lambda tag: tag.name == 'table')
        logging.info("fetching first ts for transaction history")
        rows = table.findAll(lambda tag: tag.name == 'tr')
        table_row = rows[1].findAll(lambda tag: tag.name == 'td')
        li = table_row[1].text.split(" ")
        time_stamp = li[1]+li[2]
        time_stamp = time_stamp.replace(":", ".")
        zipname = "Transaction Status.zip"
        destination_path = destination_path + "Transaction Status\\" + \
            str(current_year) + "\\" + current_month + "\\" + "Test"
        file_extraction(time_stamp, zipname, destination_path)
        WebDriverWait(driver, 90).until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "tr.odd:nth-child(1) > td:nth-child(3) > form:nth-child(1) > input:nth-child(4)"))).click()
        time_stamp = li[0]+'_'+li[1]+li[2]
        time_stamp = time_stamp.replace(":", ".")
        time_stamp = time_stamp.replace("/", ".")
        loc_change_for_zip(time_stamp, zipname, destination_path)
        WebDriverWait(driver, 90).until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "tr.even:nth-child(2) > td:nth-child(3) > form:nth-child(1) > input:nth-child(4)"))).click()
        time.sleep(1)
        table_row = rows[2].findAll(lambda tag: tag.name == 'td')
        logging.info("fetching second ts for transaction history")
        li = table_row[1].text.split(" ")
        time_stamp = li[0]+'_'+li[1]+li[2]
        time_stamp = time_stamp.replace(":", ".")
        time_stamp = time_stamp.replace("/", ".")
        loc_change_for_zip(time_stamp, zipname, destination_path)
        WebDriverWait(driver, 90).until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "tr.odd:nth-child(3) > td:nth-child(3) > form:nth-child(1) > input:nth-child(4)"))).click()
        time.sleep(1)
        table_row = rows[3].findAll(lambda tag: tag.name == 'td')
        logging.info("fetching third ts for transaction history")
        li = table_row[1].text.split(" ")
        time_stamp = li[0]+'_'+li[1]+li[2]
        time_stamp = time_stamp.replace(":", ".")
        time_stamp = time_stamp.replace("/", ".")
        loc_change_for_zip(time_stamp, zipname, destination_path)
    except Exception as e:
        logging.error('Exception caught during download_file_transaction_status() : {}'.format(str(e)))
        print('Exception caught during download_file_transaction_status() : {}'.format(str(e)))
        raise e


def download_file_transaction_history(driver, destination_path):
    try:
        driver.get(download_file_transaction_history_url)
        WebDriverWait(driver, 90).until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "tr.odd:nth-child(1) > td:nth-child(3) > form:nth-child(1) > input:nth-child(4)"))).click()
        time.sleep(1)
        soup = BeautifulSoup(driver.page_source, 'lxml')
        table = soup.find(lambda tag: tag.name == 'table')
        rows = table.findAll(lambda tag: tag.name == 'tr')
        table_row = rows[1].findAll(lambda tag: tag.name == 'td')
        li = table_row[1].text.split(" ")
        time_stamp = li[1]+li[2]
        time_stamp = time_stamp.replace(":", ".")
        zipname = "Transaction History.zip"
        destination_path = destination_path + "Transaction History\\" + \
            str(current_year) + "\\" + current_month + "\\" + "Test"
        file_extraction(time_stamp, zipname, destination_path)
    except Exception as e:
        logging.error('Exception caught during download_file_transaction_history() : {}'.format(str(e)))
        print('Exception caught during download_file_transaction_history() : {}'.format(str(e)))
        raise e


def download_file_expired_trades(driver, destination_path):
    try:
        driver.get(download_file_expired_trades_url)
        WebDriverWait(driver, 90).until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "tr.odd:nth-child(1) > td:nth-child(3) > form:nth-child(1) > input:nth-child(4)"))).click()
        time.sleep(1)
        soup = BeautifulSoup(driver.page_source, 'lxml')
        table = soup.find(lambda tag: tag.name == 'table')
        rows = table.findAll(lambda tag: tag.name == 'tr')
        table_row = rows[1].findAll(lambda tag: tag.name == 'td')
        li = table_row[1].text.split(" ")
        time_stamp = li[1]+li[2]
        time_stamp = time_stamp.replace(":", ".")
        zipname = "Expired Trades.zip"
        destination_path = destination_path + "Expired Trades\\" + \
            str(current_year) + "\\" + current_month + "\\" + "Test"
        file_extraction(time_stamp, zipname, destination_path)
    except Exception as e:
        logging.error('Exception caught during download_file_expired_trades() : {}'.format(str(e)))
        print('Exception caught during download_file_expired_trades() : {}'.format(str(e)))
        raise e


def download_file_cancelled_trades(driver, destination_path):
    try:
        driver.get(download_file_cancelled_trades_url)
        WebDriverWait(driver, 90).until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "tr.odd:nth-child(1) > td:nth-child(3) > form:nth-child(1) > input:nth-child(4)"))).click()
        time.sleep(1)
        soup = BeautifulSoup(driver.page_source, 'lxml')
        table = soup.find(lambda tag: tag.name == 'table')
        rows = table.findAll(lambda tag: tag.name == 'tr')
        table_row = rows[1].findAll(lambda tag: tag.name == 'td')
        li = table_row[1].text.split(" ")
        time_stamp = li[1]+li[2]
        time_stamp = time_stamp.replace(":", ".")
        zipname = "Cancelled Trades.zip"
        destination_path = destination_path + "CancelledTrades\\" + \
            str(current_year) + "\\" + current_month + "\\" + "Test"
        file_extraction(time_stamp, zipname, destination_path)
    except Exception as e:
        logging.error('Exception caught during download_file_cancelled_trades() : {}'.format(str(e)))
        print('Exception caught during download_file_cancelled_trades() : {}'.format(str(e)))
        raise e


def download_file_RIN_batches(driver, destination_path):
    try:
        driver.get(download_file_RIN_batches_url)
        WebDriverWait(driver, 90).until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "tr.odd:nth-child(1) > td:nth-child(3) > form:nth-child(1) > input:nth-child(4)"))).click()
        time.sleep(1)
        soup = BeautifulSoup(driver.page_source, 'lxml')
        table = soup.find(lambda tag: tag.name == 'table')
        rows = table.findAll(lambda tag: tag.name == 'tr')
        table_row = rows[1].findAll(lambda tag: tag.name == 'td')
        li = table_row[1].text.split(" ")
        time_stamp = li[1]+li[2]
        time_stamp = time_stamp.replace(":", ".")
        zipname = "RIN Batches.zip"
        destination_path = destination_path + "RIN Batches\\" + \
            str(current_year) + "\\" + current_month + "\\" + "Test"
        file_extraction(time_stamp, zipname, destination_path)
    except Exception as e:
        logging.error(
            'Exception caught during download_file_RIN_batches() : {}'.format(str(e)))
        print('Exception caught during download_file_RIN_batches() : {}'.format(str(e)))
        raise e


if __name__ == "__main__":
    try:
        job_id = np.random.randint(1000000, 9999999)
        
        logfile = os.getcwd()+'\\logs\\EMTS_DAILY_FILE_AUTOMATION_log.txt'
        logging.basicConfig(
        level=logging.INFO,
        force=True,
        format='%(asctime)s:%(levelname)s:%(name)s:%(message)s',
        filename=logfile)
        
        # Remove any existing handlers
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
        
        credential_dict = bu_config.get_config('EMTS_DAILY_FILE_AUTOMATION', 'N', other_vert=True)
        username = credential_dict['USERNAME']
        password = credential_dict['PASSWORD']
        urls = credential_dict['SOURCE_URL'].split(";")
        database = credential_dict['DATABASE'].split(";")[0]
        warehouse = credential_dict['DATABASE'].split(";")[1]
        table_name = credential_dict['TABLE_NAME']
        destination_path = credential_dict["API_KEY"]
        job_name = credential_dict['PROJECT_NAME']
        owner = credential_dict['IT_OWNER']
        receiver_email = credential_dict['EMAIL_LIST']
        
        #-----------------------URLS from config------------------------
        url_1 =urls[0]
        
        download_file_pending_trades_url = urls[1]

        download_file_pending_trades_details_url = urls[2]

        download_file_RIN_holdings_url = urls[3]

        download_file_completed_trades_url = urls[4]

        download_file_transaction_status_url = urls[5]

        download_file_transaction_history_url = urls[6]

        download_file_expired_trades_url = urls[7]

        download_file_cancelled_trades_url = urls[8]

        download_file_RIN_batches_url = urls[9]
        
        ####################### Uncommment for Testing ################################################################################################
        database = "BUITDB_DEV"
        warehouse = "BUIT_WH"
        # destination_path = r"\\biourja.local\biourja\India Sync\RINS\RINS Recon\\"
        destination_path = r"E:\\testingEnvironment\\J_local_drive\\RINS\\RINS Recon\\"
        # username = "biorins13"
        # password = "May2023@@"
        
        # url_1 = 'https://cdx.epa.gov/CDX/Login'
        
        # download_file_pending_trades_url = 'https://emts.epa.gov/emts/documentlist/viewhistory.html?catalogId=10&subscriptionId=&abt=false'

        # download_file_pending_trades_details_url = 'https://emts.epa.gov/emts/documentlist/viewhistory.html?catalogId=11&subscriptionId=&abt=false'

        # download_file_RIN_holdings_url = 'https://emts.epa.gov/emts/documentlist/viewhistory.html?catalogId=20&subscriptionId=&abt=false'

        # download_file_completed_trades_url = 'https://emts.epa.gov/emts/documentlist/viewhistory.html?catalogId=50&subscriptionId=&abt=false'

        # download_file_transaction_status_url = 'https://emts.epa.gov/emts/documentlist/viewhistory.html?catalogId=430&subscriptionId=&abt=false'

        # download_file_transaction_history_url = 'https://emts.epa.gov/emts/documentlist/viewhistory.html?catalogId=30&subscriptionId=&abt=false'

        # download_file_expired_trades_url = 'https://emts.epa.gov/emts/documentlist/viewhistory.html?catalogId=40&subscriptionId=&abt=false'

        # download_file_cancelled_trades_url = 'https://emts.epa.gov/emts/documentlist/viewhistory.html?catalogId=370&subscriptionId=&abt=false'

        # download_file_RIN_batches_url = 'https://emts.epa.gov/emts/documentlist/viewhistory.html?catalogId=380&subscriptionId=&abt=false'
        
        job_name ="BIO-PAD01_" +  job_name
        
        receiver_email = "amanullah.khan@biourja.com,yashn.jain@biourja.com,imam.khan@biourja.com,yash.gupta@biourja.com,\
        bhavana.kaurav@biourja.com,bharat.pathak@biourja.com,deep.durugkar@biourja.com"
        ###############################################################################################################################################
        
        # BU_LOG entry(started) in PROCESS_LOG table
        log_json = '[{"JOB_ID": "'+str(job_id)+'","JOB_NAME": "'+str(
            job_name)+'","CURRENT_DATETIME": "'+str(datetime.now())+'","STATUS": "STARTED"}]'
        bu_alerts.bulog(process_name=job_name, table_name=table_name, status='STARTED',
                        process_owner=owner, row_count=0, log=log_json, database=database, warehouse=warehouse)
        
        download_path = os.getcwd()+"\\temp_download\\"
        today = date.today()
        current_datetime = datetime.now() -timedelta(1)
        current_year = current_datetime.year
        current_month = current_datetime.strftime("%B")
        files = os.listdir(download_path)
        
        # removing existing files
        for file in files:
            if os.path.isfile(download_path+'\\'+file):
                os.remove(download_path+'\\'+file)

        logging.warning('info added')
        excel_files = []
        logging.info("Loading Browser")
        driver = firefoxDriverLoader()
        logging.info("Driver Loaded now logging into website")
        login(driver)
        logging.info("Login Successfull, now getting data from website")
        get_data(driver)
        logging.info(
            "Download started waiting for it to complete for pendingtrades")
        download_file_pending_trades(driver, destination_path)
        logging.info(
            "Download started waiting for it to complete for pending trade details")
        download_file_pending_trades_details(driver, destination_path)
        logging.info(
            "Download started waiting for it to complete Cancelled Trades")
        download_file_cancelled_trades(driver, destination_path)
        logging.info(
            "Download started waiting for it to complete completed Trades")
        download_file_completed_trades(driver, destination_path)
        logging.info(
            "Download started waiting for it to complete Transaction status")
        download_file_transaction_status(driver, destination_path)
        logging.info(
            "Download started waiting for it to complete Tansaction History")
        download_file_transaction_history(driver, destination_path)
        logging.info(
            "Download started waiting for it to complete Expired trades")
        download_file_expired_trades(driver, destination_path)
        logging.info(
            "Download started waiting for it to complete for Rin holdings")
        download_file_RIN_holdings(driver, destination_path)
        logging.info(
            "Download started waiting for it to complete for Rin Batches weekely file")
        download_file_RIN_batches(driver, destination_path)
        logging.info("CLosing Driver")
        driver.quit()
        a = pd.DataFrame(excel_files[0]).to_excel(
            'PendingTrade.xlsx', index=False)
        b = pd.DataFrame(excel_files[1]).to_excel(
            'CompletedTrade.xlsx', index=False)

        logging.info("Driver quit")
        multiple_attachment_list = [f"{os.getcwd()}"+"\\PendingTrade.xlsx"]+[
            f"{os.getcwd()}"+"\\"+"CompletedTrade.xlsx"] + [f'{logfile}']

        # BU_LOG entry(completed) in PROCESS_LOG table
        log_json = '[{"JOB_ID": "'+str(job_id)+'","JOB_NAME": "'+str(
            job_name)+'","CURRENT_DATETIME": "'+str(datetime.now())+'","STATUS": "COMPLETED"}]'
        bu_alerts.bulog(process_name=job_name, table_name=table_name, status='COMPLETED',
                        process_owner=owner, row_count=1, log=log_json, database=database, warehouse=warehouse)
        
        bu_alerts.send_mail(
            receiver_email=receiver_email,
            mail_subject=f'JOB SUCCESS - {job_name}',
            mail_body='EMTS_DAILY_FILE_AUTOMATION completed successfully, Attached logs',
            multiple_attachment_list=multiple_attachment_list
        )
        os.remove(f"{os.getcwd()}"+"\\PendingTrade.xlsx")
        os.remove(f"{os.getcwd()}"+"\\CompletedTrade.xlsx")
    except Exception as e:
        try:
            driver.quit()
        except:
            pass
        a = pd.DataFrame(excel_files[0]).to_excel(
            'PendingTrade.xlsx', index=False)
        b = pd.DataFrame(excel_files[1]).to_excel(
            'CompletedTrade.xlsx', index=False)
        logging.info("Driver quit")
        multiple_attachment_list = [f"{os.getcwd()}"+"\\PendingTrade.xlsx"]+[
            f"{os.getcwd()}"+"\\"+"CompletedTrade.xlsx"] + [f'{logfile}']
        logging.info(f'Error occurred in EMTS_DAILY_FILE_AUTOMATION {e}')

        # BU_LOG entry(Failed) in PROCESS_LOG table
        log_json = '[{"JOB_ID": "'+str(job_id)+'","JOB_NAME": "'+str(
            job_name)+'","CURRENT_DATETIME": "'+str(datetime.now())+'","STATUS": "FAILED"}]'
        bu_alerts.bulog(process_name=job_name, table_name=table_name, status='FAILED',
                        process_owner=owner, row_count=0, log=log_json, database=database, warehouse=warehouse)
        
        bu_alerts.send_mail(
            receiver_email=receiver_email,
            mail_subject=f"JOB FAILED - {job_name}",
            mail_body=f"{e}",
            multiple_attachment_list=multiple_attachment_list)
        sys.exit(-1)