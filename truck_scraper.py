# -*- coding: utf-8 -*-
"""
Created on Fri Feb  3 22:08:21 2023

@author: micha
"""

# url = "https://partnercarrier.com/"

# response = requests.get(url, headers = headers)
# soup = BeautifulSoup(response.text, 'html.parser')

# urlTemplate = "https://partnercarrier.com{}"
# stateTags = soup.find_all('div', attrs = {"class":"col-md-4 col-sm-6 col-xs-12 form-group"})

# stateUrlList = []
# for tag in stateTags:
#     linkTag = tag.find('a')
#     stateUrl = urlTemplate.format(linkTag['href'])
#     stateUrlList.append(stateUrl)


# cityUrlList = []
# for stateLink in stateUrlList:

    
#     responseState = requests.get(stateLink, headers = headers)
#     soupState = BeautifulSoup(responseState.text, 'html.parser')
#     cityTags = soupState.find_all('a', attrs = {'class':"city-link-font-size"})
    
#     for tag in cityTags:
#         cityUrl = urlTemplate.format(tag['href'])
#         cityUrlList.append(cityUrl)

# print(len(cityUrlList))
    

# compUrlList = []
# for cityLink in cityUrlList:
    
#     print('--NEW CITY-- : ', cityLink)
    
#     pageUrl = cityLink # define first pageUrl for the first page of results 
    

#     # Go to the next page and scrape company data until no more pages exist, then break and go to next city
#     while True:
#         print('pageUrl', pageUrl)
        
#         # get soup object for current page of current city
#         responsePage = requests.get(pageUrl, headers = headers)
#         soupPage = BeautifulSoup(responsePage.text, 'html.parser')
#         pageTags = soupPage.find_all('a', attrs = {'class':"btn btn-primary pull-right"})
        
#         # get url for each company on the current page 
#         for tag in pageTags:
#             compUrl = urlTemplate.format(tag['href'])
#             compUrlList.append(compUrl)
            
#         # update url to next page
#         nextPageTag = soupPage.find('a', attrs = {'rel':'next'})
#         if nextPageTag == None:
#             break
        
#         # Update pageUrl to next page 
#         pageUrl = urlTemplate.format(nextPageTag['href']) 
    
# print(len(compUrlList))



# visitedCities = []

# list25to32k = []
# def transform(cityLink):    
#      print('--NEW CITY-- : ', cityLink)
     
#      if cityLink in visitedCities:
#          return
#      visitedCities.append(cityLink)
        
#      pageUrl = cityLink # define first pageUrl for the first page of results 
     
    
#      # Go to the next page and scrape company data until no more pages exist, then break and go to next city
#      while True:
#          print('pageUrl - ', pageUrl)
         
#          # get soup object for current page of current city
#          responsePage = requests.get(pageUrl, headers = headers)
#          soupPage = BeautifulSoup(responsePage.text, 'html.parser')
#          pageTags = soupPage.find_all('a', attrs = {'class':"btn btn-primary pull-right"})
         
#          # get url for each company on the current page 
#          for tag in pageTags:
#              compUrl = urlTemplate.format(tag['href'])
#              list25to32k.append(compUrl)
             
#          # update url to next page
#          nextPageTag = soupPage.find('a', attrs = {'rel':'next'})
#          if nextPageTag == None:
#              break
         
#          # Update pageUrl to next page 
#          pageUrl = urlTemplate.format(nextPageTag['href']) 
#      return 


# with concurrent.futures.ThreadPoolExecutor() as executor:
#     executor.map(transform, cityUrlList[25000:])



import requests
from bs4 import BeautifulSoup
import concurrent.futures
import pandas as pd
import copy


headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"}











#requests_cache.install_cache('my_cache') 


with open("C:/Users/micha/compUrlListFull.txt") as f:
    fullList = [line[:-1] for line in f]
compUrlListFull = fullList


compUrlSet = set(compUrlListFull)
compUrlListOld = copy.deepcopy(compUrlListFull)
compUrlListFull = list(compUrlSet)


allRequests = []

def collect_requests(compUrl):
    print(compUrl)
    r = requests.get(compUrl, headers = headers)
    #s = BeautifulSoup(r.text, 'html.parser')
    allRequests.append(r.text)
    return
    
# for compUrl in compUrlListFull[:10]:
#     print(compUrl)
#     r = requests.get(compUrl, headers = headers)
#     #s = BeautifulSoup(r.text, 'html.parser')
#     allRequests.append(r.text)
        
    
with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(collect_requests, compUrlListFull[200000:400000])
        

# automatically save doc and load them back into a list and then concat the list
## -------- SAVE -------
file = open('req1.6-1.8m.txt', 'w')
for url in final200kRequests:
    file.write(url)
    file.write("`1`1`")
file.close()



## ------- LOAD -------
# MEMORY ERROR - convert to string then back to list w/ split()
reqfile = []
with open("C:/Users/micha/req300k.txt") as f:
    for line in f:
        reqfile.append(line)


with open("C:/Users/micha/req300k.txt") as f:
    reqfile = f.read()
reqList = reqfile.split("`1`1`")[:-1]







missingRowList = []

df = pd.DataFrame()
df1 = pd.DataFrame()
df2 = pd.DataFrame()

for text in allRequests[:200000]:
    s = BeautifulSoup(text, 'html.parser')
    
    nameTag = s.find('h2', attrs = {'class':'text-center'})
    if nameTag == None:
        name = 'N/A' 
    else:
        name = nameTag.text.lower().strip()
    
    addressTag = s.find('a', attrs = {'target':'_blank'})
    if addressTag == None:
        address = 'N/A'
    else:
        address = addressTag.text.lower().strip()
        
       
    tags = s.find_all('div', attrs = {'class':"col-sm-4 nopadding-left"})   
    if tags != []:
        
        if 'USDOT Number:' in str(tags[0].text):
            DOT = tags[0].text.lower().strip()
        else: DOT = 'N/A'
            
        
        if 'MC Number:' in str(tags[2].text):
            MC = tags[2].text.lower().strip()
        else: MC = 'N/A'
        
        if 'Trucks' in str(tags[3].text):
            trucks = tags[3].text.lower().strip()
            
        else: trucks = 'N/A'
        
        if 'Drivers:' in str(tags[4].text):
            drivers = tags[4].text.lower().strip()
        
        else: drivers = 'N/A'
            
        if 'Cell Phone:' in str(tags[5].text):
            cell = tags[5].text.lower().strip()
        
        else: cell = 'N/A'
            
        if 'Fax:' in str(tags[6].text):
            fax = tags[6].text.lower().strip()
            
        else: fax = 'N/A'
            
        if 'Website:' in str(tags[7].text):
            website = tags[7].text.lower().strip()
          
        else: website = 'N/A'
            
        if 'Email:' in str(tags[8].text):
            email = tags[8].text.lower().strip()
            
        else: email = 'N/A'
    
    tags2 = s.find_all('div', attrs = {'class':'col-sm-4 col-md-4 nopadding-left'})
    if tags2 != []:
        found = 0
        for tag in tags2:
            labelTag = tag.find('label')
            if str(labelTag.text.strip()) == 'Phone:':
                if tag.find('a') == None:
                    break
                found = 1
                break
            
        if found == 1:
            phoneTag = tag.find('a')
           
            phone = phoneTag.text.strip()
        elif found == 0:
            
            phone = 'N/A'
        
    
    serviceTag = s.find('div', attrs = {'class':'col-sm-12 col-md-12 nopadding-left form-group'})
    if serviceTag == None:
        serviceType = 'N/A'
       
    else:
        serviceType = serviceTag.text.lower().strip()
        
    
    cargoTag = s.find('div', attrs = {'class':"col-sm-12 col-md-12 nopadding-left"})
    if cargoTag == None:
        cargoType = 'N/A'
        
    else:
        cargoType = cargoTag.text.lower().strip()
      
    
    
    try:
        nextRow = {'Name':name, 'Address':address, 'Email':email, 
                                'Cargo Type':cargoType, 'Website':website, 'Drivers':drivers, 
                                'Trucks':trucks, 'USDOT Number':DOT, 'MC Number':MC, 
                                'Phone Number':phone, 'Service Type':serviceType}
    except:
        missingRowList.append(text)
        continue 
    
    df = df.append(nextRow, ignore_index = True) 
    #df1 = df1.append(nextRow, ignore_index = True)
    
    
    
    
# ---- SAVE TO CSV ----
df1.to_csv("C:/Users/micha/truck_df0-200kV2m.csv", index = False)


# ---- Appending to df ---
df = df.append(df1)



# --- READ CSVs ---
df200 = pd.read_csv("C:/Users/micha/truck_df0-200k.csv")
df400 = pd.read_csv("C:/Users/micha/truck_df200-400k.csv")
df600 = pd.read_csv("C:/Users/micha/truck_df400-600k.csv")
df800 = pd.read_csv("C:/Users/micha/truck_df600-800k.csv")
df1000 = pd.read_csv("C:/Users/micha/truck_df800-1m.csv")
df1200 = pd.read_csv("C:/Users/micha/truck_df1m-1.2m.csv")
df1400 = pd.read_csv("C:/Users/micha/truck_df1.2m-1.4m.csv")
df1600 = pd.read_csv("C:/Users/micha/truck_df1.4m-1.6m.csv")
df1800 = pd.read_csv("C:/Users/micha/truck_df1.6m-1.8m.csv")


# --- CREATE FULL DF --- 
dfFull = df200.append(df400)
dfFull = dfFull.append(df600)
dfFull = dfFull.append(df800)
dfFull = dfFull.append(df1000)
dfFull = dfFull.append(df1200)
dfFull = dfFull.append(df1400)
dfFull = dfFull.append(df1600)
dfFull = dfFull.append(df1800)







state = []
for adr in dfFull['Address']:
    if type(adr) != float: 
        adr = adr.split(',')[-2][1:]
    state.append(adr)
dfFull.insert(3, 'State', state) 

newDrivers = []
for driver in dfFull['Drivers']:
    newDrivers.append(driver.split('drivers: \r\n')[-1])
dfFull['Drivers'] = newDrivers


newTrucks = []
for truck in dfFull['Trucks']:
    newTrucks.append(truck.split('s: \r\n')[-1])
dfFull['Trucks'] = newTrucks 


newDOTs = []
for num in dfFull['USDOT Number']:
    newDOTs.append(num.split('number:  ')[-1])
dfFull['USDOT Number'] = newDOTs


newMCs = []
for num in dfFull['MC Number']:
    newMCs.append(num.split('number: \r\n')[-1])
dfFull['MC Number'] = newMCs



dfFull = dfFull.sort_values(by = 'State')
dfFull = dfFull.drop('Unnamed: 0', axis = 1)


df200New = dfFull[:200000]
df400New = dfFull[200000:400000]
df600New = dfFull[400000:600000]
df800New = dfFull[600000:800000]
df1000New = dfFull[800000:1000000]
df1200New = dfFull[1000000:1200000]
df1400New = dfFull[1200000:1400000]
df1600New = dfFull[1400000:1600000]
df1800New = dfFull[1600000:1801788]


dfFull.to_csv("C:/Users/micha/truck_df_full.csv", index = False)

df200New.to_csv("C:/Users/micha/df0-200k.csv", index = False)
df400New.to_csv("C:/Users/micha/df200k-400k.csv", index = False)
df600New.to_csv("C:/Users/micha/df400k-600k.csv", index = False)
df800New.to_csv("C:/Users/micha/df600k-800k.csv", index = False)
df1000New.to_csv("C:/Users/micha/df800k-1m.csv", index = False)
df1200New.to_csv("C:/Users/micha/df1m-1.2m.csv", index = False)
df1400New.to_csv("C:/Users/micha/df1.2m-1.4m.csv", index = False)
df1600New.to_csv("C:/Users/micha/df1.4m-1.6m.csv", index = False)
df1800New.to_csv("C:/Users/micha/df1.6m-1.8m.csv", index = False)












# newRequests = []
# setDOTs = set(newDOTs)
# # check for visited links 
# for url in compUrlListFullTest:
#     #print(url)
#     DOTnum1 = url.split('USDOT-')[-1]
#     #print(DOTnum1)
#     if DOTnum1 in setDOTs:
#         print('already visited, skipping')
#         continue
#     r = requests.get(url, headers = headers)
#     newRequests.append(r.text)














# -----============================================================== 
