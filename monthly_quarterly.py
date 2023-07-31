import glob 
import logging 
import os, sys ,zipfile
import shutil
import time 
from datetime import date, datetime 
import numpy as np 
import bu_alerts 
import pandas as pd 
from bs4 import BeautifulSoup
from selenium import webdriver 
from selenium.webdriver.common.by import By 
from selenium.webdriver.firefox.options import Options 
from selenium.webdriver.support import expected_conditions as EC 
from selenium.webdriver.support.ui import Select, WebDriverWait 
from selenium.webdriver.common.action_chains import ActionChains 
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary

receiver_email = "indiapowerit@biourja.com,itdevsupport@biourja.com,deepesh.gupta@biourja.com,rahul.gupta@biourja.com"
download_path = os.getcwd()+"\\download\\" 
destination_path ="J:\RINS\RINS Recon\\"
USERID = "biorins13" 
PASSWORD = "July2023@" 
JOBNAME = "EMTS_MONTHLY_FILE_AUTOMATION" 
URL ='https://cdx.epa.gov/CDX/Login' 
FIREFOX_PATH = r"C:\\Program Files\\Mozilla Firefox\\Firefox.exe"
today = date.today()
current_datetime = datetime.now()
current_year = current_datetime.year
current_month = current_datetime.strftime("%B")

logfile = os.getcwd()+'\\logs\\' + JOBNAME+"_"+str(today)+'.txt' 
if os.path.exists(logfile):
    os.remove(logfile)
files=os.listdir(download_path)
# removing existing files 
for file in files :
    if os.path.isfile(download_path+'\\'+file):
        os.remove(download_path+'\\'+file)
logging.basicConfig( 
    level=logging.INFO, 
    force= True, 
    format='%(asctime)s:%(levelname)s:%(name)s:%(message)s',
    filename=logfile) 
logging.warning('info added') 
def firefoxDriverLoader(): 
    try: 
        mime_types=['application/pdf' ,'text/plain', 'application/vnd.ms-excel', 'test/csv', 'application/zip', 'application/csv', 'text/comma-separated-values','application/download','application/octet-stream' ,'binary/octet-stream' ,'application/binary' ,'application/x-unknown'] 
        profile = webdriver.FirefoxProfile() 
        binary = FirefoxBinary(FIREFOX_PATH)
        profile.set_preference('browser.download.folderList', 2) 
        profile.set_preference('browser.download.manager.showWhenStarting', False) 
        profile.set_preference('browser.download.dir', download_path) 
        profile.set_preference('pdfjs.disabled', True) 
        profile.set_preference('browser.helperApps.neverAsk.saveToDisk', ','.join(mime_types)) 
        profile.set_preference('browser.helperApps.neverAsk.openFile',','.join(mime_types)) 
        driver = webdriver.Firefox(executable_path=os.getcwd()+'\\geckodriver.exe', firefox_binary=binary,firefox_profile = profile)  
     
        return driver 
    except Exception as e: 
        raise e 
   
def login(driver): 
    try: 
        driver.get(URL) 
        WebDriverWait(driver,90).until(EC.element_to_be_clickable((By.CSS_SELECTOR,"#LoginUserId"))).send_keys(USERID) 
        time.sleep(1) 
        WebDriverWait(driver,90).until(EC.element_to_be_clickable((By.CSS_SELECTOR,"#LoginPassword"))).send_keys(PASSWORD) 
        time.sleep(1) 
        WebDriverWait(driver,90).until(EC.element_to_be_clickable((By.CSS_SELECTOR,".node > form:nth-child(4) > input:nth-child(6)"))).click() 
    except Exception as e: 
        raise e 
def get_data(driver):
    try: 
        action = ActionChains(driver) 
        WebDriverWait(driver,90).until(EC.element_to_be_clickable((By.CSS_SELECTOR,"div.mycdx-row:nth-child(2) > div:nth-child(3) > a:nth-child(1)"))).click() 
        time.sleep(1) 
        WebDriverWait(driver,90).until(EC.element_to_be_clickable((By.CSS_SELECTOR,"#BIOURJA\ TRADING\ LLC"))).click() 
        time.sleep(1) 
       
    except Exception as e: 
        raise e 

def file_extraction(time_stamp,zipname,destination_path):
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

def file_extraction_pdf(time_stamp,zipname,destination_path):
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

def loc_change_for_zip(time_stamp,zipname,destination_path):
    for filename in os.listdir(download_path):
        filename_without_zip = filename.split('.zip')[0]
        old_zipfile_name = download_path + filename
        new_name = os.path.join(download_path,filename_without_zip +'_' + time_stamp+'.zip')
        os.rename(old_zipfile_name,new_name)
        shutil.copy(new_name,destination_path)
    os.remove(new_name)

def download_file_MonthlyTransactionHistory(driver,destination_path): 
    try: 
        action = ActionChains(driver) 
        driver.get('https://emts.epa.gov/emts/documentlist/viewhistory.html?catalogId=31&subscriptionId=&abt=false')
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
        raise e

def download_file_MonthlyRINHoldings(driver,destination_path): 
    try: 
        action = ActionChains(driver) 
        driver.get('https://emts.epa.gov/emts/documentlist/viewhistory.html?catalogId=870&subscriptionId=&abt=false')
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
        raise e
    
def download_file_RFS2EMTSRINTransaction(driver,destination_path): 
    try: 
        action = ActionChains(driver) 
        driver.get('https://emts.epa.gov/emts/documentlist/viewhistory.html?catalogId=215&subscriptionId=&abt=false')
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
        raise e
    

def download_file_RFS2EMTSActivityReportAssignedRINS(driver,destination_path): 
    try: 
        action = ActionChains(driver) 
        driver.get('https://emts.epa.gov/emts/documentlist/viewhistory.html?catalogId=240&subscriptionId=&abt=false')
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
        raise e
    

def download_file_RFS2EMTSActivityReportSeparatedRINS(driver,destination_path): 
    try: 
        action = ActionChains(driver) 
        driver.get('https://emts.epa.gov/emts/documentlist/viewhistory.html?catalogId=250&subscriptionId=&abt=false')
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
        raise e
    
if __name__ == "__main__": 
    try: 
        destination_path ="J:\RINS\RINS Recon\\"
        logging.info("Loading Browser")
        bu_alerts.bulog(process_name=JOBNAME,status='Started', log=logfile,process_owner='Pakhi',table_name=" ") 
        driver = firefoxDriverLoader() 
        logging.info("Driver Loaded now logging into website") 
        login(driver) 
        logging.info("Login Successfull, now getting data from website") 
        get_data(driver) 
        logging.info("Download started waiting for it to complete for Monthly Transaction History") 
        download_file_MonthlyTransactionHistory(driver,destination_path) 
        logging.info("Download started waiting for it to complete for Monthly RIN Holdings")
        download_file_MonthlyRINHoldings(driver,destination_path)
        logging.info("Download started waiting for it to complete Cancelled Trades")
        download_file_RFS2EMTSRINTransaction(driver,destination_path)
        logging.info("Download started waiting for it to complete completed Trades")
        download_file_RFS2EMTSActivityReportAssignedRINS(driver,destination_path)
        logging.info("Download started waiting for it to complete Transaction status")
        download_file_RFS2EMTSActivityReportSeparatedRINS(driver,destination_path)
        logging.info("CLosing Driver")
        driver.quit() 
        bu_alerts.bulog(process_name=JOBNAME,status='Finished', log=logfile,process_owner='Pakhi',table_name=" ") 
        logging.info("Driver quit")
        bu_alerts.send_mail(
                    receiver_email = receiver_email,
                    mail_subject ='JOB SUCCESS - EMTS_MONTHLY_FILE_AUTOMATION',
                    mail_body = 'EMTS_DAILY_FILE_AUTOMATION completed successfully, Attached logs',
                    attachment_location = logfile
                )
    except Exception as e:
        logging.info(f'Error occurred in EMTS_DAILY_FILE_AUTOMATION {e}')
        bu_alerts.bulog(process_name=JOBNAME,status='failed',log=logfile,process_owner='Pakhi',table_name=" ")
        bu_alerts.send_mail(
                            receiver_email= receiver_email,
                            mail_subject=f"JOB FAILED - EMTS_MONTHLY_FILE_AUTOMATION",
                            mail_body=f"{e}",
                            attachment_location = logfile)
   