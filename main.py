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
from time import sleep
import json
import requests
import re
import sys
import html
import gzip
import csv
import os
from time import time

# ## Output dir (set to /data/out/tables for KBC)
OUTPUT_DIR = sys.argv[1] if len(sys.argv) == 2 else "/data/out/tables"

## Set default time limit, 10 minutes
TIME_LIMIT = 600

## List of already processed links
LINKS_PROCESSED = []

PROCESSED_TABLES = {}

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

## Return bytes ungzipped
def ungzip(b):
    with BytesIO(b) as gzf:
        with gzip.open(gzf) as gz:
            return gz.read()


## Read config file
with open("/data/config.json" if os.path.exists("/data/config.json") else "./config.json", "r") as conf_file:
    conf = json.load(conf_file)["parameters"]

if conf["debug"]:
    print("Applied configuration:")
    print(conf)

## Print debug info
if conf["incremental"]:
    for mapping in conf["mapping"]:
        output_date = (datetime.today() + timedelta(days=int(mapping["date-condition"]))).replace(hour=0, minute=0, second=0, microsecond=0)
        if mapping["date-comparison"] != "BETWEEN":
            print("Will download only files with date {0}{1} conforming expression {2}".format(mapping["date-comparison"], output_date, mapping["matching"]))
        else:
            upper_date = (datetime.today() + timedelta(days=int(mapping["date-condition-upper"]))).replace(hour=0, minute=0, second=0, microsecond=0)
            print("Will download only files with date BETWEEN {0} AND {1} conforming expression {2}".format(output_date, upper_date, mapping["matching"]))


## Get mapping from configuration matching the link
def get_output_mapping(link):
    for mapping in conf["mapping"]:
        if re.match(mapping["matching"], link):
            return mapping
    return None

## Get date from string based on mapping
def get_mapping_date(link):
    m = get_output_mapping(link)
    if m == None:
        return None
    else:
        try:
            date = re.search(m["date-search"], link).group(1)
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
    comp_date = (datetime.today() + timedelta(days=int(m["date-condition"]))).replace(hour=0, minute=0, second=0, microsecond=0)
    if m["date-comparison"] == "<":
        return d < comp_date
    elif m["date-comparison"] == ">":
        return d > comp_date
    elif m["date-comparison"] == "=":
        return d == comp_date
    elif m["date-comparison"] == "!=":
        return d != comp_date
    elif m["date-comparison"] == "BETWEEN":
        upper_date = (datetime.today() + timedelta(days=int(m["date-condition-upper"]))).replace(hour=0, minute=0, second=0, microsecond=0)
        return (d >= comp_date and d <= upper_date)
    else:
        return False

def generate_dates_between(start, end):
    """Generate list of dates in range from start to end
    Arguments:
        start {datetime or str} -- Start datetime or str in format %Y-%m-%d
        end {[type]} -- End datetime or str in format %Y-%m-%d
    Returns:
        list -- List of dates
    """
    if not isinstance(start, datetime):
        start = datetime.strptime(start, "%Y-%m-%d")
    if not isinstance(end, datetime):
        end = datetime.strptime(end, "%Y-%m-%d")
    if start > end:
        _s = start
        start = end
        end = start
    size = (end - start).days
    dates = [end - timedelta(days=i) for i in range(size)]

    return dates


## Basic processors for different file types
## CSV processor simply appends to an existing csv file or creates a new one should it not already exist
def process_csv(fname, output, content, link):
    output = os.path.join(OUTPUT_DIR, output)
    if os.path.exists(output):
        appending = True
    else:
        appending = False
    if conf["debug"]: print("Processing CSV file {0}".format(output))
    with StringIO(content.decode("utf8")) as infile:
        csv_in = csv.reader(infile)
        ## Append or create new file and write
        with open(output, "a" if appending else "w", newline='\n', encoding="utf-8") as outfile:
            csv_out = csv.writer(outfile)
            rownum = 0
            for row in csv_in:
                rownum+=1
                if rownum == 1:
                    if not output in PROCESSED_TABLES:
                        PROCESSED_TABLES[output] = len(row)
                if appending and rownum == 1:
                    ## Skip header
                    continue
                if len(row)>PROCESSED_TABLES[output]:
                    print("Number of columns missmatch, expected {0}, got {1}".format(PROCESSED_TABLES[output], len(row)))
                    print("Omitting last columns")
                    row = row[:PROCESSED_TABLES[output]]
                elif len(row)<PROCESSED_TABLES[output]:
                    print("Number of columns missmatch, expected {0}, got {1}".format(PROCESSED_TABLES[output], len(row)))
                    print("Filling with NULLs")
                    actual_columns = len(row)
                    for appended_row_id in range(PROCESSED_TABLES[output]-actual_columns):
                        row.append("")
                if rownum == 1:
                    ## Add columns to header
                    if conf["generate-pk"]:
                        row.append(conf["primary-key"])
                    if conf["add-filename"]:
                        row.append("original_file")
                else:
                    ## Add appended columns data
                    if conf["generate-pk"]:
                        pk = "{0}-{1}".format(os.path.basename(link), rownum-1)
                        if conf["hash-pk"]:
                            pk = md5(pk.encode("utf8")).hexdigest().upper()
                        row.append(pk)
                    if conf["add-filename"]:
                        row.append(os.path.basename(link))
                ## Encode row to 'utf-8'
                #row = list(map(str.encode,row))
                csv_out.writerow(row)
        if conf["debug"]: print("Written {0} rows".format(rownum))

## TXT processor checks whether the format conforms with TSV, converts to CSV and then processes it via CSV processor
def process_txt(fname, output, content, link):
    output = os.path.join(OUTPUT_DIR, output)
    if os.path.exists(output):
        appending = True
    else:
        appending = False
    if conf["debug"]: print("Processing TXT file {0}".format(output))
    with StringIO(content.decode("utf8")) as infile:
        csv_in = csv.reader(infile, dialect="excel-tab")
        ## Append or create new file and write
        with open(output, "a" if appending else "w", newline='\n', encoding="utf-8") as outfile:
            csv_out = csv.writer(outfile, dialect="excel")
            rownum = 0
            for row in csv_in:
                rownum+=1
                if rownum == 1:
                    if not output in PROCESSED_TABLES:
                        PROCESSED_TABLES[output] = len(row)
                if len(row)>PROCESSED_TABLES[output]:
                    print("Number of columns missmatch, expected {0}, got {1}".format(PROCESSED_TABLES[output], len(row)))
                    print("Omitting last columns")
                    row = row[:PROCESSED_TABLES[output]]
                elif len(row)<PROCESSED_TABLES[output]:
                    print("Number of columns missmatch, expected {0}, got {1}".format(PROCESSED_TABLES[output], len(row)))
                    print("Filling with NULLs")
                    actual_columns = len(row)
                    for appended_row_id in range(PROCESSED_TABLES[output]-actual_columns):
                        row.append("")
                if appending and rownum == 1:
                    ## Skip header
                    continue
                if rownum == 1:
                    ## Add columns to header
                    if conf["generate-pk"]:
                        row.append(conf["primary-key"])
                    if conf["add-filename"]:
                        row.append("original_file")
                else:
                    ## Add appended columns data
                    if conf["generate-pk"]:
                        pk = "{0}-{1}".format(os.path.basename(link), rownum-1)
                        if conf["hash-pk"]:
                            pk = md5(pk.encode("utf8")).hexdigest().upper()
                        row.append(pk)
                    if conf["add-filename"]:
                        row.append(os.path.basename(link))
                ## Encode row to 'utf-8'
                #row = list(map(str.encode,row))
                #print("Number of columns: {0}".format(len(row)))
                csv_out.writerow(row)
        if conf["debug"]: print("Written {0} rows".format(rownum))

## List of accepted file types with proper handler
typelist = {
    ".csv": process_csv,
    ".txt": process_txt
}

def get_link_list(session):
    global LINKS_PROCESSED
    ## Initial call params
    params = {}
    for p in conf["endpoint"]["parameters"]:
        params = {**params, **p}


    url = conf["endpoint"]["url"]

    if "{date}" in url and conf["incremental"]:
        dates = generate_dates_between(output_date, upper_date)
    else:
        dates = [""]

    links = []
    for date in dates:
        if isinstance(date, datetime):
            date = date.strftime("%Y-%m-%d")
        current_url = url.replace("{date}", date)

        __try_number = 0
        while True:
            try:
                resp = session.get(current_url, params=params)
                break
            ## TODO: Too broad Exception handling
            except Exception as e:
                __try_number += 1
                print("ConnectionError occurred", e)
                if __try_number < 10:
                    print("Waiting few seconds before retry.")
                    sleep(10)
                    continue
                else:
                    raise Exception("Too many retries when getting link list")

        ## Check for response status and exit if non-200
        if resp.status_code != 200:
            print("Error making primary request:")
            print(resp.status_code)
            sys.exit(1)

        ## Find all links based on re specified in config
        ## The links look basically like:
        ## <url>LINK</url>
        current_links = re.findall(conf["re-match"], resp.text)
        links.extend(current_links)

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
            #if conf["debug"]: print("No output mapping found for \'{0}\'. Skipping".format(url))
            continue
        elif url in LINKS_PROCESSED:
            ## Already processed
            continue
        else:
            if conf["incremental"]:
                if get_date_conforms(link):
                    final_links.append((link, url, urlp))
                else:
                    #if conf["debug"]: print("File \'{0}\' does not meet increment condition and will be skipped.".format(url))
                    pass
            else:
                final_links.append((link, url, urlp))
    return final_links


def process_all_links(session, final_links):
    global LINKS_PROCESSED
    print("Will process {0} links".format(len(final_links)))
    start_time = time()
    completed = True
    ## Download every link from the list
    for link, url, urlp in final_links:
        if time()-start_time > TIME_LIMIT:
            completed = False
            print("Timeout of {0} seconds reached, starting over.".format(TIME_LIMIT))
            print("So far, {0} links were processed".format(len(LINKS_PROCESSED)))
            break
        if url in LINKS_PROCESSED: continue
        LINKS_PROCESSED.append(url)
        ## Check whether gzipped and supported
        if os.path.splitext(urlp.path)[1].lower() == ".gz":
            compressed = True
            ftype = os.path.splitext(urlp.path[:-3])[1]
            basename = os.path.split(urlp.path[:-3])[1]
        else:
            compressed = False
            ftype = urlp.path.splitext(urlp.path)
            basename = os.path.split(urlp.path)[1]

        if ftype not in typelist.keys():
            print("\'{0}\' is an unsupported type and cannot be processed. Path: \'{1}\'".format(ftype, urlp.path))
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
        attempt_no = 1
        while True:
            try:
                resp = session.get(link)
                break
            except Exception as e:
                print("Error occurred when downloading link {0}".format(link))
                print(e)
                if attempt_no == 5:
                    print("Already tried too many times, giving up...")
                    raise Exception("Too many errors")
                print("Retrying in 5 seconds...")
                sleep(5)
                attempt_no += 1
                continue

        ## Check response for non-ok state
        if resp.status_code != 200:
            print("Link: \'{0}\' returned status code {1}.".format(link, resp.status_code))
            if conf["abort-on-error"]:
                print("This is fatal. If you prefer not to exit on first error, set \'abort-on-error\' option to \'false\'.")
                sys.exit(1)
        
        ## Un-gzip received bytes (if needed) and call proper handler
        if compressed:
            data = ungzip(resp.content)
        else:
            data  = resp.text
        
        typelist[ftype](basename, outname, data,url)
    
    return completed

## Create requests session to keep cookies etc.
session = requests.session()

while True:
    ## Get links without already processed ones
    final_links = get_link_list(session)
    ## Process all new links, returns true if all was done, False on time limit reach
    if process_all_links(session, final_links):
        break
    else:
        continue
