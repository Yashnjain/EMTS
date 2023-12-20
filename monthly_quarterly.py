import os
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
from datetime import date, datetime , timedelta
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.support.ui import  WebDriverWait
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC
import sys



def firefoxDriverLoader(): 
    try: 
        mime_types=['application/pdf' ,'text/plain', 'application/vnd.ms-excel', 'test/csv', 'application/zip', 'application/csv', 'text/comma-separated-values','application/download','application/octet-stream' ,'binary/octet-stream' ,'application/binary' ,'application/x-unknown'] 
        profile = webdriver.FirefoxProfile() 
        profile.set_preference('browser.download.folderList', 2) 
        profile.set_preference('browser.download.manager.showWhenStarting', False) 
        profile.set_preference('browser.download.dir', download_path) 
        profile.set_preference('pdfjs.disabled', True) 
        profile.set_preference('browser.helperApps.neverAsk.saveToDisk', ','.join(mime_types)) 
        profile.set_preference('browser.helperApps.neverAsk.openFile',','.join(mime_types)) 
        driver = webdriver.Firefox(executable_path=GeckoDriverManager().install(),firefox_profile = profile)  
        return driver 
    except Exception as e:
        logging.error('Exception caught during firefoxDriverLoader() : {}'.format(str(e)))
        print('Exception caught during firefoxDriverLoader() : {}'.format(str(e)))
        raise e


def login(driver): 
    try: 
        driver.get(url_1) 
        WebDriverWait(driver,90).until(EC.element_to_be_clickable((By.CSS_SELECTOR,"#LoginUserId"))).send_keys(username) 
        time.sleep(1) 
        WebDriverWait(driver,90).until(EC.element_to_be_clickable((By.CSS_SELECTOR,"#LoginPassword"))).send_keys(password) 
        time.sleep(1) 
        WebDriverWait(driver,90).until(EC.element_to_be_clickable((By.CSS_SELECTOR,".node > form:nth-child(4) > input:nth-child(6)"))).click() 
    except Exception as e:
        logging.error('Exception caught during login() : {}'.format(str(e)))
        print('Exception caught login() : {}'.format(str(e)))
        raise e


def get_data(driver):
    try: 
        action = ActionChains(driver) 
        WebDriverWait(driver,90).until(EC.element_to_be_clickable((By.CSS_SELECTOR,"div.mycdx-row:nth-child(2) > div:nth-child(3) > a:nth-child(1)"))).click() 
        time.sleep(1) 
        WebDriverWait(driver,90).until(EC.element_to_be_clickable((By.CSS_SELECTOR,"#BIOURJA\ TRADING\ LLC"))).click() 
        time.sleep(1) 
    except Exception as e:
        logging.error('Exception caught during get_data() method : {}'.format(str(e)))
        print('Exception caught during get_data() method : {}'.format(str(e)))
        raise e


def file_extraction(time_stamp,zipname,destination_path):
    try:
        zip_file = download_path + zipname
        extract_dir = download_path
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        for filename in os.listdir(extract_dir):
            if filename.endswith('.csv') and not filename.endswith("AM.csv") and not filename.endswith("PM.csv"):
                filename_without_csv = filename.split('.csv')[0]
                old_filename = os.path.join(extract_dir,filename)
                file = os.path.join(extract_dir,filename_without_csv +'_' + time_stamp + '.xlsx')
                df = pd.read_csv(old_filename)
                if(df.empty):
                    logging.info("file is empty")
                    os.remove(zip_file)
                    os.remove(old_filename)
                    return
                else:
                    os.remove(old_filename)
                    df.to_excel(file,index=False)
                    try:
                        shutil.copy(file, destination_path)
                    except FileNotFoundError:
                        os.makedirs(destination_path, exist_ok=True)
                        shutil.copy(file, destination_path)
        os.remove(zip_file)
        os.remove(file)
    except Exception as e:
        logging.error('Exception caught during file_extraction() method : {}'.format(str(e)))
        print('Exception caught during file_extraction() method : {}'.format(str(e)))
        raise e


def file_extraction_pdf(time_stamp,zipname,destination_path):
    try:
        zip_file = download_path + zipname
        extract_dir = download_path
        print(time_stamp)
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        for filename in os.listdir(extract_dir):
            if filename.endswith('.pdf'):
                file = os.path.join(extract_dir,filename)
                try:
                    shutil.copy(file, destination_path)
                except FileNotFoundError:
                    os.makedirs(destination_path, exist_ok=True)
                    shutil.copy(file, destination_path)
        os.remove(zip_file)
        os.remove(file)
    except Exception as e:
        logging.error('Exception caught during file_extraction_pdf() : {}'.format(str(e)))
        print('Exception caught during file_extraction_pdf() : {}'.format(str(e)))
        raise e


def loc_change_for_zip(time_stamp,zipname,destination_path):
    try:
        for filename in os.listdir(download_path):
            filename_without_zip = filename.split('.zip')[0]
            print(zipname)
            old_zipfile_name = download_path + filename
            new_name = os.path.join(download_path,filename_without_zip +'_' + time_stamp+'.zip')
            os.rename(old_zipfile_name,new_name)
            try:
                shutil.copy(new_name, destination_path)
            except FileNotFoundError:
                os.makedirs(destination_path, exist_ok=True)
                shutil.copy(new_name, destination_path)
        os.remove(new_name)
    except Exception as e:
        logging.error('Exception caught during loc_change_for_zip() : {}'.format(str(e)))
        print('Exception caught during loc_change_for_zip() : {}'.format(str(e)))
        raise e


def download_file_monthly_transaction_history(driver,destination_path): 
    try: 
        action = ActionChains(driver) 
        driver.get(download_file_monthly_transaction_history_url)
        WebDriverWait(driver,90).until(EC.element_to_be_clickable((By.CSS_SELECTOR,"tr.odd:nth-child(1) > td:nth-child(3) > form:nth-child(1) > input:nth-child(4)"))).click() 
        time.sleep(1)
        soup = BeautifulSoup(driver.page_source, 'lxml')
        table = soup.find(lambda tag: tag.name=='table')
        rows = table.findAll(lambda tag: tag.name=='tr')
        table_row = rows[1].findAll(lambda tag: tag.name =='td')
        li = table_row[1].text.split(" ")
        time_stamp = li[1]+li[2]
        time_stamp = time_stamp.replace(":",".")
        zipname = "Monthly Transaction History.zip"
        destination_path = destination_path + "Monthly Transaction History\\" + str(current_year) + "\\" + "Test"
        file_extraction(time_stamp,zipname,destination_path)
    except Exception as e:
        logging.error('Exception caught during download_file_monthly_transaction_history() : {}'.format(str(e)))
        print('Exception caught during download_file_monthly_transaction_history() : {}'.format(str(e)))
        raise e


def download_file_monthly_RIN_holdings(driver,destination_path): 
    try: 
        action = ActionChains(driver) 
        driver.get(download_file_monthly_RIN_holdings_url)
        WebDriverWait(driver,90).until(EC.element_to_be_clickable((By.CSS_SELECTOR,"tr.odd:nth-child(1) > td:nth-child(3) > form:nth-child(1) > input:nth-child(4)"))).click() 
        time.sleep(1)
        soup = BeautifulSoup(driver.page_source, 'lxml')
        table = soup.find(lambda tag: tag.name=='table')
        rows = table.findAll(lambda tag: tag.name=='tr')
        table_row = rows[1].findAll(lambda tag: tag.name =='td')
        li = table_row[1].text.split(" ")
        time_stamp = li[1]+li[2]
        time_stamp = time_stamp.replace(":",".")
        zipname = "Monthly RIN Holdings.zip"
        destination_path = destination_path + "Monthly RIN Holdings\\" + str(current_year) + "\\" + "Test"
        file_extraction(time_stamp,zipname,destination_path)
    except Exception as e:
        logging.error('Exception caught during download_file_monthly_RIN_holdings() : {}'.format(str(e)))
        print('Exception caught during download_file_monthly_RIN_holdings() : {}'.format(str(e)))
        raise e
    
    
def download_file_RFS2_EMTS_RIN_transaction(driver,destination_path): 
    try: 
        action = ActionChains(driver) 
        driver.get(download_file_RFS2_EMTS_RIN_transaction_url)
        WebDriverWait(driver,90).until(EC.element_to_be_clickable((By.CSS_SELECTOR,"tr.odd:nth-child(1) > td:nth-child(3) > form:nth-child(1) > input:nth-child(4)"))).click() 
        time.sleep(1)
        soup = BeautifulSoup(driver.page_source, 'lxml')
        table = soup.find(lambda tag: tag.name=='table')
        rows = table.findAll(lambda tag: tag.name=='tr')
        table_row = rows[1].findAll(lambda tag: tag.name =='td')
        li = table_row[1].text.split(" ")
        time_stamp = li[0]+'_'+li[1]+li[2]
        time_stamp = time_stamp.replace(":",".")
        zipname = "RFS2 EMTS RIN Generation CSV_XML Report.zip"
        destination_path = destination_path + "EMTS QUARTERLY Reports\\" + str(current_year) + "\\" + "Test"
        file_extraction(time_stamp,zipname,destination_path)

    except Exception as e:
        logging.error('Exception caught during download_file_RFS2_EMTS_RIN_transaction() : {}'.format(str(e)))
        print('Exception caught during download_file_RFS2_EMTS_RIN_transaction() : {}'.format(str(e)))
        raise e
    

def download_file_RFS_EMTS_activity_report_assigned_RINS(driver,destination_path): 
    try: 
        action = ActionChains(driver) 
        driver.get(download_file_RFS_EMTS_activity_report_assigned_RINS_url)
        WebDriverWait(driver,90).until(EC.element_to_be_clickable((By.CSS_SELECTOR,"tr.odd:nth-child(1) > td:nth-child(3) > form:nth-child(1) > input:nth-child(4)"))).click() 
        time.sleep(1)
        soup = BeautifulSoup(driver.page_source, 'lxml')
        table = soup.find(lambda tag: tag.name=='table')
        rows = table.findAll(lambda tag: tag.name=='tr')
        table_row = rows[1].findAll(lambda tag: tag.name =='td')
        li = table_row[1].text.split(" ")
        time_stamp = li[0]+'_'+li[1]+li[2]
        time_stamp = time_stamp.replace(":",".")
        time_stamp = time_stamp.replace("/",".")
        zipname = "RFS2 EMTS Activity Report (Assigned RINS).zip"
        destination_path = destination_path + "Activity Report (Assigned RINS)\\" + str(current_year) + "\\" + "Test"
        file_extraction_pdf(time_stamp,zipname,destination_path)
    except Exception as e:
        logging.error('Exception caught during download_file_RFS_EMTS_activity_report_assigned_RINS() : {}'.format(str(e)))
        print('Exception caught during download_file_RFS_EMTS_activity_report_assigned_RINS() : {}'.format(str(e)))
        raise e
    

def download_file_RFS2_EMTS_activity_report_separated_RINS(driver,destination_path): 
    try: 
        action = ActionChains(driver) 
        driver.get(download_file_RFS2_EMTS_activity_report_separated_RINS_url)
        WebDriverWait(driver,90).until(EC.element_to_be_clickable((By.CSS_SELECTOR,"tr.odd:nth-child(1) > td:nth-child(3) > form:nth-child(1) > input:nth-child(4)"))).click() 
        time.sleep(1)
        soup = BeautifulSoup(driver.page_source, 'lxml')
        table = soup.find(lambda tag: tag.name=='table')
        rows = table.findAll(lambda tag: tag.name=='tr')
        table_row = rows[1].findAll(lambda tag: tag.name =='td')
        li = table_row[1].text.split(" ")
        time_stamp = li[0]+'_'+li[1]+li[2]
        time_stamp = time_stamp.replace(":",".")
        zipname = "RFS2 EMTS Activity Report (Separated RINS).zip"
        destination_path = destination_path + "Activity Report (Separated RINS)" + str(current_year) + "\\" + "Test"
        file_extraction_pdf(time_stamp,zipname,destination_path)
    except Exception as e:
        logging.error('Exception caught during download_file_RFS2_EMTS_activity_report_separated_RINS() : {}'.format(str(e)))
        print('Exception caught during download_file_RFS2_EMTS_activity_report_separated_RINS() : {}'.format(str(e)))
        raise e


if __name__ == "__main__": 
    try:
        job_id = np.random.randint(1000000, 9999999)
        logfile = os.getcwd()+'\\logs\\EMTS_MONTHLY_FILE_AUTOMATION_log.txt'
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] - %(message)s',
            filename=logfile)
        # Remove any existing handlers
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
    
        credential_dict = bu_config.get_config('EMTS_MONTHLY_FILE_AUTOMATION', 'N', other_vert=True)
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

        url_1 = urls[0]
        
        download_file_monthly_transaction_history_url =  urls[1]

        download_file_monthly_RIN_holdings_url = urls[2]

        download_file_RFS2_EMTS_RIN_transaction_url = urls[3]

        download_file_RFS_EMTS_activity_report_assigned_RINS_url = urls[4]

        download_file_RFS2_EMTS_activity_report_separated_RINS_url = urls[5]
        
        ####################### Uncommment for Testing #############################################
        database = "BUITDB_DEV"
        warehouse = "BUIT_WH"
        # destination_path = r"\\biourja.local\biourja\India Sync\RINS\RINS Recon\\"
        # destination_path = r"E:\\testingEnvironment\\J_local_drive\\RINS\\RINS Recon\\"
        # username = "biorins13"
        # password = "May2023@@"
        
        # url_1 ='https://cdx.epa.gov/CDX/Login' 
        # download_file_monthly_transaction_history_url =  'https://emts.epa.gov/emts/documentlist/viewhistory.html?catalogId=31&subscriptionId=&abt=false'

        # download_file_monthly_RIN_holdings_url = 'https://emts.epa.gov/emts/documentlist/viewhistory.html?catalogId=870&subscriptionId=&abt=false'

        # download_file_RFS2_EMTS_RIN_transaction_url = 'https://emts.epa.gov/emts/documentlist/viewhistory.html?catalogId=215&subscriptionId=&abt=false'

        # download_file_RFS_EMTS_activity_report_assigned_RINS_url = 'https://emts.epa.gov/emts/documentlist/viewhistory.html?catalogId=240&subscriptionId=&abt=false'

        # download_file_RFS2_EMTS_activity_report_separated_RINS_url = 'https://emts.epa.gov/emts/documentlist/viewhistory.html?catalogId=250&subscriptionId=&abt=false'

        job_name ="BIO-PAD01_" +  job_name
        # receiver_email = "amanullah.khan@biourja.com,yashn.jain@biourja.com,imam.khan@biourja.com,yash.gupta@biourja.com,\
        # bhavana.kaurav@biourja.com,bharat.pathak@biourja.com,deep.durugkar@biourja.com"
        
        ############################################################################################
        download_path = os.getcwd()+"\\download\\" 
        today = date.today()
        current_datetime = datetime.now() -timedelta(1)
        current_year = current_datetime.year
        current_month = current_datetime.strftime("%B")
        files=os.listdir(download_path)
        # removing existing files 
        for file in files :
            if os.path.isfile(download_path+'\\'+file):
                os.remove(download_path+'\\'+file)
                
        # BU_LOG entry(started) in PROCESS_LOG table
        log_json = '[{"JOB_ID": "'+str(job_id)+'","JOB_NAME": "'+str(
            job_name)+'","CURRENT_DATETIME": "'+str(datetime.now())+'","STATUS": "STARTED"}]'
        bu_alerts.bulog(process_name=job_name, table_name=table_name, status='STARTED',
                        process_owner=owner, row_count=0, log=log_json, database=database, warehouse=warehouse)

        logging.info("Loading Browser")
        driver = firefoxDriverLoader()
        logging.info("Driver Loaded now logging into website") 
        login(driver)
        logging.info("Login Successfull, now getting data from website")
        get_data(driver)
        
        logging.info("Download started waiting for it to complete for Monthly Transaction History")
        download_file_monthly_transaction_history(driver,destination_path) 
        
        logging.info("Download started waiting for it to complete for Monthly RIN Holdings")
        download_file_monthly_RIN_holdings(driver,destination_path)
        
        logging.info("Download started waiting for it to complete Cancelled Trades")
        download_file_RFS2_EMTS_RIN_transaction(driver,destination_path)
        
        logging.info("Download started waiting for it to complete completed Trades")
        download_file_RFS_EMTS_activity_report_assigned_RINS(driver,destination_path)
        
        logging.info("Download started waiting for it to complete Transaction status")
        download_file_RFS2_EMTS_activity_report_separated_RINS(driver,destination_path)
        
        logging.info("CLosing Driver")
        driver.quit()
        logging.info("Driver quit")
        print("Prcoess Completed")
        # BU_LOG entry(completed) in PROCESS_LOG table
        log_json = '[{"JOB_ID": "'+str(job_id)+'","JOB_NAME": "'+str(
            job_name)+'","CURRENT_DATETIME": "'+str(datetime.now())+'","STATUS": "COMPLETED"}]'
        bu_alerts.bulog(process_name=job_name, table_name=table_name, status='COMPLETED',
                        process_owner=owner, row_count=1, log=log_json, database=database, warehouse=warehouse)
        
        bu_alerts.send_mail(
                    receiver_email = receiver_email,
                    mail_subject =f'JOB SUCCESS - {job_name}',
                    mail_body = f'{job_name} completed successfully, Attached logs',
                    attachment_location = logfile
                )

    except Exception as e:
        logging.info(f'Error occurred in EMTS_DAILY_FILE_AUTOMATION {e}')
        print(f'Error occurred in EMTS_DAILY_FILE_AUTOMATION {e}')
        log_json = '[{"JOB_ID": "'+str(job_id)+'","JOB_NAME": "'+str(
        job_name)+'","CURRENT_DATETIME": "'+str(datetime.now())+'","STATUS": "FAILED"}]'
        bu_alerts.bulog(process_name=job_name, table_name=table_name, status='FAILED',
                        process_owner=owner, row_count=0, log=log_json, database=database, warehouse=warehouse)
        bu_alerts.send_mail(
                            receiver_email= receiver_email,
                            mail_subject=f"JOB FAILED - {job_name}",
                            mail_body=f"{e}",
                            attachment_location = logfile)
        sys.exit(-1)
     finally:
        try:
            bu_alerts.send_mail(
                            receiver_email= receiver_email,
                            mail_subject=f"JOB FAILED - {job_name}",
                            mail_body=f"{e}",
                            attachment_location = logfile)
            driver.quit()
        except:
            driver.quit()
            pass