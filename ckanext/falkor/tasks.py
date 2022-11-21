from ckanext import falkor
import json
import logging
import requests
import ckan.model as models
import ckanext.falkor

import os
import sys

file_dir = os.path.dirname(__file__)
sys.path.append(file_dir)

import json

#from pylons import config
from ckan.plugins.toolkit import config

log = logging.getLogger(__name__)



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




def notify_hooks_resource_create_cheaty(resource, site_url):
    
   
    log.info('Firing webhooks')
    log.info(resource)
    

    resData = "name = " + resource['name']

    docID = resourceIDMap(resource['id'])

    #userID = userIDMap(webhook['user_id'])

    payload = {
        'data': resData,
        'documentId': docID
    }

    data1 = json.dumps(payload)

    r = requests.post("https://test.falkor.byzgen.com/api/core/v0/3/dataset/" + resource['package_id'] + "/create", headers={
            'Content-Type': 'application/json',
            "accept": "application/json",
            "Authorization": "Bearer thckfSRdQN2oGUKrcU1A4kXmFSg6acBjLsG+mltWNMY2YjQzMjk0ZmQwOTZhNmY0"
        },
        json = {
            'data': resData,
            'documentId': docID
            },
        timeout=2
    )



def notify_hooks_resource_create(resource, webhook, site_url):
    log.info('Firing webhooks for {0}'.format(webhook['topic']))

    data = "name = " + resource['name'] + ", title = " + resource['title']

    payload = {
        'data': data,
        'documentId': resource['id'],
        'userId': webhook['user_id'],
    }

    #Commands that could be useful in upcoming development

    #payload = {
    #    'entity': resource['id'],
    #    'address': webhook['address'],
    #    'webhook_id': webhook['id'],
    #    'ckan': site_url,
    #    'topic': webhook['topic'],
    #    'userId': webhook['user_id'],
    #}
    #requests.post(webhook['address'], headers={
    #        'Content-Type': 'application/json',
    #        'Authorization': 'Bearer "add later for sec"'
    #    },
    #    data=json.dumps(payload),
    #    timeout=2
    #)
    #requests.post("http://86.24.210.143:3001/api/core/v0/3/dataset/4/create", headers={
    #        'Content-Type': 'application/json',
    #        'Authorization': 'Bearer "add later for sec"'
    #    },
    #    data=json.dumps(payload),
    #    timeout=2
    #)

    requests.post("http://86.24.210.143:3001/api/core/v0/3/dataset/4/create", headers={
            'Content-Type': 'application/json',
        },
        data=json.dumps(payload),
        timeout=2
    )

    log.info('Fired webhooks for {0}'.format(webhook['topic']))

def notify_hooks_resource_update(resource, webhook, site_url):
    log.info('Firing webhooks for {0}'.format(webhook['topic']))

    data = "name = " + resource['name'] + ", title = " + resource['title']

    payload = {
        'data': data,
        'documentId': resource['id'],
        'userId': webhook['user_id'],
    }

    requests.post("http://86.24.210.143:3001/api/core/v0/v0/3/dataset/4/" + resource['id'] +"/body", headers={
            'Content-Type': 'application/json',
        },
        data=json.dumps(payload),
        timeout=2
    )

    log.info('Fired webhooks for {0}'.format(webhook['topic']))

def notify_hooks_resource_read(userID, datasetID, documentID, site_url):

    log.info('Fired webhooks for Document/Read')

    payload = {
        'userId': userID,
        'datasetID': datasetID,
        'documentId': documentID,
    }

    #requests.post("http://86.24.210.143:3001/api/core/v0/:tenantId/dataset/:datasetId/create", headers={
    #       'Content-Type': 'application/json',
    #    },
    #    data=json.dumps(payload),
    #    timeout=2
    #)

def notify_hooks_resource_delete(resource, webhook, site_url):
    log.info('Firing webhooks for {0}'.format(webhook['topic']))

    data = "name = " + resource['name'] + ", title = " + resource['title']

    payload = {
        'data': data,
        'documentId': resource['id'],
        'userId': webhook['user_id'],
    }

    requests.post("http://86.24.210.143:3001/api/core/v0/v0/3/dataset/4/" + resource['id'] + "/body", headers={
            'Content-Type': 'application/json',
        },
        data=json.dumps(payload),
        timeout=2
    )

    log.info('Fired webhooks for {0}'.format(webhook['topic']))

def notify_hooks_dataset_create(resource):
    log.info('Firing dataset creation event to falkor')

    payload = {
        'datasetId': str(resource['id']),
        "encryptionType": "none",
        "externalStorage": "false",
        "permissionEnabled": "false",
        "taggingEnabled": "false",
        "iotaEnabled": "false",
        "tokensEnabled": "false"
    }

    log.debug(payload)

    r = requests.post("https://test.falkor.byzgen.com/api/admin/v0/3/dataset", headers={
            'Content-Type': 'application/json',
            "accept": "application/json",
            "Authorization": "Bearer thckfSRdQN2oGUKrcU1A4kXmFSg6acBjLsG+mltWNMY2YjQzMjk0ZmQwOTZhNmY0"
        },
        data=json.dumps(payload),
        timeout=2
    )

    log.debug(r)



