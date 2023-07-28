import os
import sys
import time
import shutil
import zipfile
import logging
import bu_alerts
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from datetime import date, datetime
from bu_config import config as buconfig
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.action_chains import ActionChains 
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary
from webdriver_manager.firefox import GeckoDriverManager


def firefoxDriverLoader():
    try: 
        mime_types=['application/pdf' ,'text/plain', 'application/vnd.ms-excel', 'test/csv', 'application/zip', 'application/csv', 'text/comma-separated-values','application/download','application/octet-stream' ,'binary/octet-stream' ,'application/binary' ,'application/x-unknown'] 
        profile = webdriver.FirefoxProfile()
        binary = FirefoxBinary(firefox_path)
        profile.set_preference('browser.download.folderList', 2) 
        profile.set_preference('browser.download.manager.showWhenStarting', False) 
        profile.set_preference('browser.download.dir', download_path)
        profile.set_preference('pdfjs.disabled', True) 
        profile.set_preference('browser.helperApps.neverAsk.saveToDisk', ','.join(mime_types)) 
        profile.set_preference('browser.helperApps.neverAsk.openFile',','.join(mime_types)) 
        driver = webdriver.Firefox(executable_path=GeckoDriverManager().install(), firefox_binary=binary,firefox_profile = profile)
        return driver
    except Exception as e:
        print(f"Exception caught in firefoxDriverLoader method: {e}")
        logging.info(f"Exception caught in firefoxDriverLoader method: {e}")
        raise e


def login(driver):
    try: 
        driver.get(source_url) 
        WebDriverWait(driver,90).until(EC.element_to_be_clickable((By.CSS_SELECTOR,"#LoginUserId"))).send_keys(user_id) 
        time.sleep(1) 
        WebDriverWait(driver,90).until(EC.element_to_be_clickable((By.CSS_SELECTOR,"#LoginPassword"))).send_keys(password)
        time.sleep(1) 
        WebDriverWait(driver,90).until(EC.element_to_be_clickable((By.CSS_SELECTOR,".node > form:nth-child(4) > input:nth-child(6)"))).click()
    except Exception as e:
        print(f"Exception caught in login method: {e}")
        logging.info(f"Exception caught in login method: {e}")
        raise e


def get_data(driver):
    try: 
        action = ActionChains(driver) 
        WebDriverWait(driver,90).until(EC.element_to_be_clickable((By.CSS_SELECTOR,"div.mycdx-row:nth-child(2) > div:nth-child(3) > a:nth-child(1)"))).click() 
        time.sleep(1)
        WebDriverWait(driver,90).until(EC.element_to_be_clickable((By.CSS_SELECTOR,"#BioUrja\ Renewables\,\ LLC"))).click() 
        time.sleep(1)
    except Exception as e:
        print(f"Exception caught in get_data: {e}")
        logging.info(f"Exception caught in get_data: {e}")
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
                    shutil.copy(file,destination_path)
        os.remove(zip_file)
        os.remove(file)
    except Exception as e:
        print(f"Exception caught in file_extraction: {e}")
        logging.info(f"Exception caught in file_extraction: {e}")
        raise e


def file_extraction_pdf(time_stamp,zipname,destination_path):
    try:
        zip_file = download_path + zipname
        extract_dir = download_path
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        for filename in os.listdir(extract_dir):
            if filename.endswith('.pdf'):
                file = os.path.join(extract_dir,filename)
                shutil.copy(file,destination_path)
        os.remove(zip_file)
        os.remove(file)
    except Exception as e:
        print(f"Exception caught in file_extraction_pdf: {e}")
        logging.info(f"Exception caught in file_extraction_pdf: {e}")
        raise e


def loc_change_for_zip(time_stamp,zipname,destination_path):
    try:
        for filename in os.listdir(download_path):
            filename_without_zip = filename.split('.zip')[0]
            old_zipfile_name = download_path + filename
            new_name = os.path.join(download_path,filename_without_zip +'_' + time_stamp+'.zip')
            os.rename(old_zipfile_name,new_name)
            shutil.copy(new_name,destination_path)
        os.remove(new_name)
    except Exception as e:
        print(f"Exception caught in loc_change_for_zip: {e}")
        logging.info(f"Exception caught in loc_change_for_zip: {e}")
        raise e


def download_file_MonthlyTransactionHistory(driver,destination_path): 
    try: 
        driver.get(base_url+str('catalogId=31&subscriptionId=&abt=false'))
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
        destination_path = destination_path + "\\Monthly Transaction History\\"  + "Test"
        file_extraction(time_stamp,zipname,destination_path)

    except Exception as e:
        print(f"Exception caught in download_file_MonthlyTransactionHistory: {e}")
        logging.info(f"Exception caught in download_file_MonthlyTransactionHistory: {e}")
        raise e


def download_file_MonthlyRINHoldings(driver,destination_path): 
    try: 
        action = ActionChains(driver) 
        driver.get(base_url+str('catalogId=870&subscriptionId=&abt=false'))
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
        destination_path = destination_path + "\\Monthly RIN Holdings\\"  + "Test"
        file_extraction(time_stamp,zipname,destination_path)

    except Exception as e:
        print(f"Exception caught in download_file_MonthlyRINHoldings: {e}")
        logging.info(f"Exception caught in download_file_MonthlyRINHoldings: {e}")
        raise e


def download_file_MonthlyRINGeneration(driver,destination_path): 
    try: 
        action = ActionChains(driver) 
        driver.get(base_url+str('catalogId=690&subscriptionId=&abt=false'))
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
        zipname = "Monthly RIN Generation.zip"
        destination_path = destination_path + "\\Monthly RIN Generation\\Test"
        file_extraction(time_stamp,zipname,destination_path)

    except Exception as e:
        print(f"Exception caught in download_file_MonthlyRINGeneration: {e}")
        logging.info(f"Exception caught in download_file_MonthlyRINGeneration: {e}")
        raise e
    

def download_file_RFS2EMTSActivityReportAssignedRINS(driver,destination_path): 
    try: 
        action = ActionChains(driver) 
        driver.get(base_url+str('catalogId=240&subscriptionId=&abt=false'))
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
        destination_path = destination_path + "\\EMTS Activity Report (Assigned RINS)\\Test"
        file_extraction_pdf(time_stamp,zipname,destination_path)

    except Exception as e:
        print(f"Exception caught in download_file_RFS2EMTSActivityReportAssignedRINS: {e}")
        logging.info(f"Exception caught in download_file_RFS2EMTSActivityReportAssignedRINS: {e}")
        raise e
    

def download_file_RFS2EMTSActivityReportSeparatedRINS(driver,destination_path): 
    try: 
        action = ActionChains(driver) 
        driver.get(base_url+str('catalogId=250&subscriptionId=&abt=false'))
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
        destination_path = destination_path + "\\EMTS Activity Report (Separated RINS)\\Test"
        file_extraction_pdf(time_stamp,zipname,destination_path)

    except Exception as e:
        print(f"Exception caught in download_file_RFS2EMTSActivityReportSeparatedRINS: {e}")
        logging.info(f"Exception caught in download_file_RFS2EMTSActivityReportSeparatedRINS: {e}")
        raise e


def download_file_RFS2EMTSRINGenerationCSV_XMLReport(driver,destination_path):
    try:
        action = ActionChains(driver) 
        driver.get(base_url+str('catalogId=215&subscriptionId=&abt=false'))
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
        zipname = "RFS2 EMTS RIN Generation CSV_XML Report.zip"
        destination_path = destination_path + "\\RFS2 EMTS RIN Generation CSV_XML Report\\Test"
        file_extraction(time_stamp,zipname,destination_path)

    except Exception as e:
        print(f"Exception caught in download_file_RFS2EMTSRINGenerationCSV_XMLReport: {e}")
        logging.info(f"Exception caught in download_file_RFS2EMTSRINGenerationCSV_XMLReport: {e}")
        raise e


def download_file_RFS2EMTSRINTransactionCSV_XMLReport(driver,destination_path):
    try:
        action = ActionChains(driver) 
        driver.get(base_url+str('catalogId=225&subscriptionId=&abt=false'))
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
        zipname = "RFS2 EMTS RIN Transaction CSV_XML Report (Buy, Sell, Separate, Retire).zip"
        destination_path = destination_path + "\\RFS2 EMTS RIN Transaction CSV_XML Report (Buy, Sell, Separate, Retire)\\Test"
        file_extraction(time_stamp,zipname,destination_path)

    except Exception as e:
        print(f"Exception caught in download_file_RFS2EMTSRINTransactionCSV_XMLReport: {e}")
        logging.info(f"Exception caught in download_file_RFS2EMTSRINTransactionCSV_XMLReport: {e}")
        raise e
    

if __name__ == "__main__": 
    try:
        
        # Generate the random job id
        job_id = np.random.randint(1000000, 9999999)

        # configure log file
        logfile = os.getcwd()+r'\\logs\\main_renewables_monthly.txt'

        # Remove any existing handlers
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
            
        # configure the basicConfig
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] - %(message)s',
            filename=logfile)
        
        # Getting credential using bu_config
        credential_dict = buconfig.get_config(
            'EMTS_MONTHLY_FILE_AUTOMATION_RENEWABLES', 'N', other_vert=True)
        user_id =credential_dict['USERNAME']
        password =credential_dict['PASSWORD']
        table_name = credential_dict['TABLE_NAME']
        owner = credential_dict['IT_OWNER']
        database = credential_dict['DATABASE'].split(";")[0]
        warehouse = credential_dict['DATABASE'].split(";")[1]
        urls= credential_dict['SOURCE_URL'].split(";")
        job_name = credential_dict['PROJECT_NAME']
        receiver_email = credential_dict['EMAIL_LIST']
        root_loc = credential_dict['API_KEY']

        source_url= urls[0]
        base_url = urls[1]
        
        ###################### Uncomment for Testing ###############################################
        database = "BUITDB_DEV"
        warehouse = "BUIT_WH"
        # base_url ="https://emts.epa.gov/emts/documentlist/viewhistory.html?"
        # source_url= "https://cdx.epa.gov/CDX/Login"
        job_name =  "BIO_PAD01_" + job_name
        receiver_email = "amanullah.khan@biourja.com,yashn.jain@biourja.com,imam.khan@biourja.com,yash.gupta@biourja.com,\
        bhavana.kaurav@biourja.com,bharat.pathak@biourja.com,deep.durugkar@biourja.com"
        root_loc = r"E:\testingEnvironment\J_local_drive\RINS\BioUrja Renewables\EMTS REPORTS"
        ##########################################################################################
        
        destination_path =root_loc
        download_path = os.getcwd()+"\\download_renewables\\" 

        firefox_path = r"C:\\Program Files\\Mozilla Firefox\\Firefox.exe"
        today = date.today()
        current_datetime = datetime.now()
        current_year = current_datetime.year
        current_month = current_datetime.strftime("%B")

        if os.path.exists(logfile):
                    os.remove(logfile)
        files=os.listdir(download_path)
        # removing existing files 
        for file in files :
            if os.path.isfile(download_path+'\\'+file):
                        os.remove(download_path+'\\'+file)
        
        logging.warning('info added')
        logging.info("Loading Browser")
        
        # BU_LOG entry(started) in PROCESS_LOG table
        log_json = '[{"JOB_ID": "'+str(job_id)+'","JOB_NAME": "'+str(
            job_name)+'","CURRENT_DATETIME": "'+str(datetime.now())+'","STATUS": "STARTED"}]'
        bu_alerts.bulog(process_name=job_name, table_name=table_name, status='STARTED',
                        process_owner=owner, row_count=0, log=log_json, database=database, warehouse=warehouse)
        
        driver = firefoxDriverLoader() 
        logging.info("Driver Loaded now logging into website") 
        login(driver) 
        logging.info("Login Successfull, now getting data from website") 
        get_data(driver) 
        logging.info("Download started waiting for it to complete for Monthly Transaction History") 
        download_file_MonthlyTransactionHistory(driver,destination_path) 
        logging.info("Download started waiting for it to complete for Monthly RIN Holdings")
        download_file_MonthlyRINHoldings(driver,destination_path)
        logging.info("Download started waiting for it to complete Monthly RIN Generation")
        download_file_MonthlyRINGeneration(driver,destination_path)
        logging.info("Download started waiting for it to complete RFS2 EMTS Activity Report Assigned RINS")
        download_file_RFS2EMTSActivityReportAssignedRINS(driver,destination_path)
        logging.info("Download started waiting for it to complete RFS2 EMTS Activity Report Seperated RINS")
        download_file_RFS2EMTSActivityReportSeparatedRINS(driver,destination_path)
        logging.info("Download started waiting for it to complete RFS2EMTSRINGenerationCSV/XMLReport")
        download_file_RFS2EMTSRINGenerationCSV_XMLReport(driver,destination_path)
        logging.info("Download started waiting for it to complete RFS2EMTSRINGenerationCSV/XMLReport")
        download_file_RFS2EMTSRINTransactionCSV_XMLReport(driver,destination_path)
        logging.info("CLosing Driver")
        try:
            driver.quit()
        except:
            pass
        
        # BU_LOG entry(completed) in PROCESS_LOG table
        log_json = '[{"JOB_ID": "'+str(job_id)+'","JOB_NAME": "'+str(
            job_name)+'","CURRENT_DATETIME": "'+str(datetime.now())+'","STATUS": "COMPLETED"}]'
        bu_alerts.bulog(process_name=job_name, table_name=table_name, status='COMPLETED',
                        process_owner=owner, row_count=0, log=log_json, database=database, warehouse=warehouse)
        

        bu_alerts.send_mail(
                    receiver_email = receiver_email,
                    mail_subject ='JOB SUCCESS - {job_name}',
                    mail_body = f'{job_name} completed successfully, Attached logs',
                    attachment_location = logfile
                )
    except Exception as e:
        try:
            driver.quit()
        except:
            pass
        logging.info(f'Error occurred in {job_name} {e}')
        # BU_LOG entry(failed) in PROCESS_LOG table
        
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
   