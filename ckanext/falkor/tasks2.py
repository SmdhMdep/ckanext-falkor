#updated verson of tasks

from urllib import response

import json
import requests
import ckan.model as models
import ckan.plugins.toolkit as toolkit
import ckan.lib.jobs as jobs
import ckanext.falkor

import os
import sys

import logging
log = logging.getLogger(__name__)

# Constants
try:
    TenantID = toolkit.config.get('ckan.falkor_tenant_id')
except:
    TenantID = 3 

# Base header constant
baseHeaders = {
            'Content-Type': 'application/json',
            "accept": "application/json",
            "Authorization": "Bearer thckfSRdQN2oGUKrcU1A4kXmFSg6acBjLsG+mltWNMY2YjQzMjk0ZmQwOTZhNmY0"
        }

# Send a post request to falkor
def falkorPost(url, payload, headers):
    response = requests.post(url, headers = headers,json = payload,timeout=2)
    return response

# Send a post request to falkor
def falkorPut(url, payload, headers):
    response = requests.put(url, headers = headers,json = payload,timeout=2)
    return response

def documentCreation(resource):
    # Format data for falkor
    url = "https://test.falkor.byzgen.com/api/core/v0/"+ TenantID +"/dataset/" + resource['package_id'] + "/create"
    payload = {
            'documentId': resourceIDMap(resource['id']),
            'data': "name = " + resource['name']
            }

    #run async request
    jobs.enqueue(
        falkorPost,
        [url, payload, baseHeaders]
    )

# Cannot be used till falkor can deal with:
#   document UUIDS 
#   JSON document updates
def documentUpdate(resource):
    # Format data for falkor
    url = "https://test.falkor.byzgen.com/api/core/v0/"+ TenantID +"/dataset/"+ resource['package_id'] +"/"+ resource['id'] +"/body"
    payload = {
            'data': "name = " + resource['name']
            }

    #run async request
    jobs.enqueue(
        falkorPut,
        [url, payload, baseHeaders]
    )

def datasetCreation(resource):
    # Format data for falkor
    url = "https://test.falkor.byzgen.com/api/admin/v0/"+ TenantID +"/dataset"
    payload = {
        'datasetId': str(resource['id']),
        "encryptionType": "none",
        "externalStorage": "false",
        "permissionEnabled": "false",
        "taggingEnabled": "false",
        "iotaEnabled": "false",
        "tokensEnabled": "false"
    }

    #run async request
    jobs.enqueue(
        falkorPost,
        [url, payload, baseHeaders]
    )

#cheaty version method delete later
#Maps CKAN UUID to a int. data is stored in a text file
def resourceIDMap(resourceID):

    returnValue = 0

    jsonPath = str(os.path.dirname(os.path.realpath(__file__))) + "/" + "resourceMap.json" 

    with open(jsonPath, 'r') as openfile:
        # Reading from json file
        json_object = json.load(openfile)
    
    #check if the map exist for current ID
    #IF it does return value
    #else create new key and value
    if resourceID in json_object:
        returnValue = json_object[resourceID]
    else:
        returnValue = len(json_object) + 1
        json_object[resourceID] = returnValue

    with open(jsonPath, "w") as outfile:
        json.dump(json_object, outfile)


    log.info("CONVERT")
    return returnValue

#cheaty version method delete later
#Maps CKAN UUID to a int. data is stored in a text file
def userIDMap(userID):

    returnValue = 0

    jsonPath = str(os.path.dirname(os.path.realpath(__file__))) + "/" + "userMap.json" 

    with open(jsonPath, 'r') as openfile:
        # Reading from json file
        json_object = json.load(openfile)
    
    #check if the map exist for current ID
    #IF it does return value
    #else create new key and value
    if userID in json_object:
        returnValue = json_object[userID]
    else:
        returnValue = len(json_object) + 1
        json_object[userID] = returnValue

    with open(jsonPath, "w") as outfile:
        json.dump(json_object, outfile)


    log.info("CONVERT")
    return returnValue

#cheaty version method delete later
#Maps CKAN UUID to a int. data is stored in a text file
def packageIDMap(packageID):

    returnValue = 0

    jsonPath = str(os.path.dirname(os.path.realpath(__file__))) + "/" + "packageMap.json" 

    with open(jsonPath, 'r') as openfile:
        # Reading from json file
        json_object = json.load(openfile)
    
    #check if the map exist for current ID
    #IF it does return value
    #else create new key and value
    if packageID in json_object:
        returnValue = json_object[packageID]
    else:
        returnValue = len(json_object) + 1
        json_object[packageID] = returnValue

    with open(jsonPath, "w") as outfile:
        json.dump(json_object, outfile)


    log.info("CONVERT")
    return returnValue