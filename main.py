#!/usr/bin/env python3
# -*- coding: utf-8 -*-

############################################
## URL-list extractor
## (c) by Tomas Votava, BizzTreat, 2019
############################################

## Imports
from io import BytesIO, StringIO
from urllib.parse import urlparse, urljoin
from dateutil.parser import parse as dateparse
from datetime import datetime, timedelta
from hashlib import md5
import json
import requests
import re
import sys
import html
import gzip
import csv
import os

## Output dir (set to /data/out/tables for KBC)
OUTPUT_DIR = "/data/out/tables"

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

## Return bytes ungzipped
def ungzip(b):
    with BytesIO(b) as gzf:
        with gzip.open(gzf) as gz:
            return gz.read()


## Read config file
with open("/data/config.json" if os.path.exists("/data/config.json") else "conf.json","r") as conf_file:
    conf = json.load(conf_file)["parameters"]

## Get mapping from configuration matching the link
def get_output_mapping(link):
    for mapping in conf["mapping"]:
        if re.match(mapping["matching"],link):
            return mapping
    return None

## Get date from string based on mapping
def get_mapping_date(link):
    m = get_output_mapping(link)
    if m == None:
        return None
    else:
        try:
            date = re.search(m["date-search"],link).group(1)
            return dateparse(date)
        except Exception as e:
            print(e)
            if conf["abort-on-error"]:
                print("This is fatal. If you prefer not to exit on first error, set \'abort-on-error\' option to \'false\'.")
                sys.exit(1)

## Gets matching output filename by re given in config
def get_output_filename(fname):
    m = get_output_mapping(fname)
    if m == None:
        return None
    else:
        return m["output"]

def get_date_conforms(link):
    m = get_output_mapping(link)
    d = get_mapping_date(link)
    if m==None or d==None: return None
    comp_date = (datetime.today() + timedelta(days=int(m["date-condition"])))
    if m["date-comparison"]=="<":
        return d < comp_date
    elif m["date-comparison"]==">":
        return d > comp_date
    elif m["date-comparison"]=="=":
        return d == comp_date
    elif m["date-comparison"]=="!=":
        return d != comp_date
    else:
        return False


## Basic processors for different file types
## CSV processor simply appends to an existing csv file or creates a new one should it not already exist
def process_csv(fname,output,content,link):
    output = os.path.join(OUTPUT_DIR,output)
    if os.path.exists(output):
        appending = True
    else:
        appending = False
    if conf["debug"]: print("Processing CSV file {0}".format(output))
    with StringIO(content.decode("utf8")) as infile:
        csv_in = csv.reader(infile)
        ## Append or create new file and write
        with open(output,"a" if appending else "w",newline='\n') as outfile:
            csv_out = csv.writer(outfile)
            rownum = 0
            for row in csv_in:
                rownum+=1
                if appending and rownum==1:
                    ## Skip header
                    continue
                if rownum==1:
                    ## Add columns to header
                    if conf["generate-pk"]:
                        row.append(conf["primary-key"])
                    if conf["add-filename"]:
                        row.append("original_file")
                else:
                    ## Add appended columns data
                    if conf["generate-pk"]:
                        pk = "{0}-{1}".format(link,rownum-1)
                        if conf["hash-pk"]:
                            pk = md5(pk.encode("utf8")).hexdigest().upper()
                        row.append(pk)
                    if conf["add-filename"]:
                            row.append(link)
                csv_out.writerow(row)
        if conf["debug"]: print("Written {0} rows".format(rownum))

## TXT processor checks whether the format conforms with TSV, converts to CSV and then processes it via CSV processor
def process_txt(fname,output,content,link):
    output = os.path.join(OUTPUT_DIR,output)
    if os.path.exists(output):
        appending = True
    else:
        appending = False
    if conf["debug"]: print("Processing TXT file {0}".format(output))
    with StringIO(content.decode("utf8")) as infile:
        csv_in = csv.reader(infile,dialect="excel-tab")
        ## Append or create new file and write
        with open(output,"a" if appending else "w",newline='\n') as outfile:
            csv_out = csv.writer(outfile,dialect="excel")
            rownum = 0
            for row in csv_in:
                rownum+=1
                if appending and rownum==1:
                    ## Skip header
                    continue
                csv_out.writerow(row)
        if conf["debug"]: print("Written {0} rows".format(rownum))

## List of accepted file types with proper handler
typelist = {
    ".csv": process_csv,
    ".txt": process_txt
}


## Create requests session to keep cookies etc.
session = requests.session()

## Initial call params
params = {}
for p in conf["endpoint"]["parameters"]:
    params = {**params, **p}


url = conf["endpoint"]["url"]

resp = session.get(url,params=params)

## Check for response status and exit if non-200
if resp.status_code != 200:
    print("Error making primary request:")
    print(resp.status_code)
    sys.exit(1)

## Find all links based on re specified in config
## The links look basically like:
## <url>LINK</url>
links = re.findall(conf["re-match"],resp.text)

## But they might be different some time
if len(links)==0:
    print("Error - No URLs match given expression")
    sys.exit(1)

## Output number of links for feedback reasons
print("Found {0} links in total".format(len(links)))

final_links = []

for link in links:
    link = html.unescape(link)
    urlp = urlparse(link)
    url = urlp.scheme + "://" + urlp.netloc + urlp.path
    # print("Path: {0}\ntranslated: {1}\nOutput: {2}\nDate: {3}\nConforms: {4}".format(urlp.path,url,get_output_filename(url),get_mapping_date(url),get_date_conforms(url)))
    if (get_output_mapping(url) == None):
        if conf["debug"]: print("No output mapping found for \'{0}\'. Skipping".format(url))
        continue
    else:
        if conf["incremental"]:
            if get_date_conforms(link):
               final_links.append((link,url,urlp))
            else:
                if conf["debug"]: print("File \'{0}\' does not meet increment condition and will be skipped.".format(url))
        else:
            final_links.append((link,url,urlp))

print("Will process {0} links".format(len(final_links)))

## Download every link from the list
for link,url,urlp in final_links:
    ## Check whether gzipped and supported
    if os.path.splitext(urlp.path)[1].lower()==".gz":
        compressed = True
        ftype = os.path.splitext(urlp.path[:-3])[1]
        basename = os.path.split(urlp.path[:-3])[1]
    else:
        compressed = False
        ftype = urlp.path.splitext(urlp.path)
        basename = os.path.split(urlp.path)[1]

    if ftype not in typelist.keys():
        print("\'{0}\' is an unsupported type and cannot be processed. Path: \'{1}\'".format(ftype,urlp.path))
        if conf["abort-on-error"]:
            print("This is fatal. If you prefer not to exit on first error, set \'abort-on-error\' option to \'false\'.")
            sys.exit(1)
    
    ## Find output mapping from config
    outname = get_output_filename(basename)

    ## No matching re
    if outname == None:
        print("File \'{0}\' does not match any of the output mapping setting.".format(basename))
        if conf["abort-on-error"]:
            print("This is fatal. If you prefer not to exit on first error, set \'abort-on-error\' option to \'false\'.")
            sys.exit(1)

    ## Download the file
    resp = session.get(link)

    ## Check response for non-ok state
    if resp.status_code != 200:
        print("Link: \'{0}\' returned status code {1}.".format(link,resp.status_code))
        if conf["abort-on-error"]:
            print("This is fatal. If you prefer not to exit on first error, set \'abort-on-error\' option to \'false\'.")
            sys.exit(1)
    
    ## Un-gzip received bytes (if needed) and call proper handler
    if compressed:
        data = ungzip(resp.content)
    else:
        data  = resp.text
    
    typelist[ftype](basename,outname,data,url)
