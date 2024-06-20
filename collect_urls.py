"""
This script collects all the URLs and saves them to the local machine to be used later in web_scraper_main.py
"""

import requests
from bs4 import BeautifulSoup
import concurrent.futures
import pandas as pd
import copy

url = "https://partnercarrier.com/"

response = requests.get(url, headers = headers)
soup = BeautifulSoup(response.text, 'html.parser')

urlTemplate = "https://partnercarrier.com{}"
stateTags = soup.find_all('div', attrs = {"class":"col-md-4 col-sm-6 col-xs-12 form-group"})

stateUrlList = []
for tag in stateTags:
    linkTag = tag.find('a')
    stateUrl = urlTemplate.format(linkTag['href'])
    stateUrlList.append(stateUrl)


cityUrlList = []
for stateLink in stateUrlList:

    
    responseState = requests.get(stateLink, headers = headers)
    soupState = BeautifulSoup(responseState.text, 'html.parser')
    cityTags = soupState.find_all('a', attrs = {'class':"city-link-font-size"})
    
    for tag in cityTags:
        cityUrl = urlTemplate.format(tag['href'])
        cityUrlList.append(cityUrl)

print(len(cityUrlList))
    

compUrlList = []
for cityLink in cityUrlList:
    
    print('--NEW CITY-- : ', cityLink)
    
    pageUrl = cityLink # define first pageUrl for the first page of results 
    

    # Go to the next page and scrape company data until no more pages exist, then break and go to next city
    while True:
        print('pageUrl', pageUrl)
        
        # get soup object for current page of current city
        responsePage = requests.get(pageUrl, headers = headers)
        soupPage = BeautifulSoup(responsePage.text, 'html.parser')
        pageTags = soupPage.find_all('a', attrs = {'class':"btn btn-primary pull-right"})
        
        # get url for each company on the current page 
        for tag in pageTags:
            compUrl = urlTemplate.format(tag['href'])
            compUrlList.append(compUrl)
            
        # update url to next page
        nextPageTag = soupPage.find('a', attrs = {'rel':'next'})
        if nextPageTag == None:
            break
        
        # Update pageUrl to next page 
        pageUrl = urlTemplate.format(nextPageTag['href']) 
    
print(len(compUrlList))



visitedCities = []

list25to32k = []
def transform(cityLink):    
     print('--NEW CITY-- : ', cityLink)
     
     if cityLink in visitedCities:
         return
     visitedCities.append(cityLink)
        
     pageUrl = cityLink # define first pageUrl for the first page of results 
     
    
     # Go to the next page and scrape company data until no more pages exist, then break and go to next city
     while True:
         print('pageUrl - ', pageUrl)
         
         # get soup object for current page of current city
         responsePage = requests.get(pageUrl, headers = headers)
         soupPage = BeautifulSoup(responsePage.text, 'html.parser')
         pageTags = soupPage.find_all('a', attrs = {'class':"btn btn-primary pull-right"})
         
         # get url for each company on the current page 
         for tag in pageTags:
             compUrl = urlTemplate.format(tag['href'])
             list25to32k.append(compUrl)
             
         # update url to next page
         nextPageTag = soupPage.find('a', attrs = {'rel':'next'})
         if nextPageTag == None:
             break
         
         # Update pageUrl to next page 
         pageUrl = urlTemplate.format(nextPageTag['href']) 
     return 


with concurrent.futures.ThreadPoolExecutor() as executor:
    executor.map(transform, cityUrlList[25000:])
