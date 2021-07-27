
import os
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time
#import pyautogui as g
import numpy as np
import pandas as pd
from selenium.webdriver.common.action_chains import ActionChains
from multiprocessing import Process, Value, Array, Lock

import argparse

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--process', default=1, type=int) 
    parser.add_argument('--start', default=0, type=int) 
    parser.add_argument('--end', default=None, type=int) 
    parser.add_argument("--outputdir",default="tempoutput",type=str)

    args = parser.parse_args()
    if args.end == None:
        args.end = args.start+100
    
    return args



def crawl(titles:list,lock_print,lock_file,procNum:int,outputdir:str):
    driver = webdriver.Chrome('./chromedriver.exe')
    for idx,winename in enumerate(titles):
        try:
            lock_print.acquire()
            print('process number: %d wine idx:%d || '%(procNum,idx),end='')
            lock_print.release()
            driver.get('https://www.vivino.com/')
            elem_input = driver.find_element_by_class_name('searchBar__searchInput--2Nf0D')
            elem_input.send_keys(winename)
            elem_input.submit()
            driver.implicitly_wait(1)
            try:
                elem_image = driver.find_element_by_class_name('link-color-alt-grey')
            except:
                lock_print.acquire()
                print('%s is skipped. check ./skipped.txt file'%(winename))
                lock_print.release()

                lock_file.acquire()
                f = open('./skipped.txt','a+')
                f.write(winename+'\n')
                f.close()
                lock_file.release()
                continue
            link = elem_image.get_attribute('href')
            driver.get(link)
            driver.implicitly_wait(3)

            body = driver.find_element_by_css_selector('body')
            for i in range(8):
                body.send_keys(Keys.PAGE_DOWN)

            driver.implicitly_wait(4)
            try:
                driver.find_element_by_link_text('Show more reviews').click()
            except:
                lock_print.acquire()
                print('%s is skipped. check ./skipped.txt file'%(winename))
                lock_print.release()

                lock_file.acquire()
                f = open('./skipped.txt','a+')
                f.write(winename+'\n')
                f.close()
                lock_file.release()
                continue

            try:
                driver.implicitly_wait(1)
                comm_review = driver.find_element_by_class_name('allReviews__header--1AKxx')
            except:
                lock_print.acquire()
                print('%s is skipped. check ./skipped.txt file'%(winename))
                lock_print.release()

                lock_file.acquire()
                f = open('./skipped.txt','a+')
                f.write(winename+'\n')
                f.close()
                lock_file.release()
                continue

            actions = ActionChains(driver)
            actions.move_to_element(comm_review).click().perform()
            for i in range(50):
                actions.send_keys(Keys.END).perform()

            time.sleep(1)
            data = driver.find_elements_by_class_name('communityReview__reviewText--2bfLj') 
            time.sleep(2)
            l = []
            for d in data:
                l.append(d.text)

            dfWine = pd.DataFrame(np.array(l))
            lock_print.acquire()
            print("winename: %s number of reviews: %d "%(winename,len(dfWine)))
            lock_print.release()


            dfWine.to_csv('./%s/%s.csv'%(outputdir,winename))
        except KeyboardInterrupt as e:
            break
        except Exception as e:
            print(e)
            continue

def main():
    args = get_args()
    if not os.path.exists(args.outputdir):
        os.mkdir(args.outputdir)

    df = pd.read_csv('./title_130k_rmYear_lower_sorted.csv')
    names = df['0'].values.tolist()
    nProcess = args.process 
    procs = []
    lock_print = Lock()
    lock_file = Lock()
    start = args.start 
    dist = args.end // nProcess
    outputdir = args.outputdir

    for i in range(nProcess):
        if i == nProcess-1:
            p = Process(target=crawl,args=(names[start:args.end],lock_print,lock_file,i,outputdir))
        else:
            p = Process(target=crawl,args=(names[start:start+dist],lock_print,lock_file,i,outputdir))
        p.start()
        procs.append(p)
        start += dist
    
    for p in procs:
        p.join()
    
    print('crawling is finished. start:%d end:%d'%(start,dist*nProcess))

if __name__ == "__main__":
    main()