import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit

import logging

import os
import sys

file_dir = os.path.dirname(__file__)
sys.path.append(file_dir)

import uuid
import json
import requests
import ckan.model as model
import ckanext.falkor

#new command for 2.9
#from pylons import config
from ckan.plugins.toolkit import config

import ckan.lib.jobs as jobs
from ckan.lib.dictization import table_dictize
from ckan.model.domain_object import DomainObjectOperation

from ckanext.falkor import tasks

log = logging.getLogger(__name__)


class FalkorPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IDomainObjectModification, inherit=True)

    # IConfigurer
    def update_config(self, config_):
        toolkit.add_template_directory(config_, 'templates')
        toolkit.add_public_directory(config_, 'public')
        toolkit.add_resource('fanstatic',
            'falkor')

    
    #IDomainObjectNotification & #IResourceURLChange
    #not running as jobs right now as mapping UUID's to ints cannot be done in jobs

    def notify(self, entity, operation=None):
        context = {'model': model, 'ignore_auth': True, 'defer_commit': True}

        website = "http://ec2-18-134-94-243.eu-west-2.compute.amazonaws.com:5000/"

        if isinstance(entity, model.Resource):
            if not operation:
                #This happens on IResourceURLChange, but I'm not sure whether
                #to make this into a webhook.
                return

            #resource/document create
            elif operation == DomainObjectOperation.new:
                topic = 'resource/create'
                resource = table_dictize(entity, context)
                tasks.notify_hooks_resource_create_cheaty(resource, website)

                #jobs.enqueue(
                #    tasks.notify_hooks_resource_create_cheaty,
                #    [resource, webhook, website]
                #)

            #resource/document update
            if operation == DomainObjectOperation.changed:
                topic = 'resource/update'

                resource = table_dictize(entity, context)

                #tasks.notify_hooks_resource_update(resource, webhook)

                #jobs.enqueue(
                #    tasks.notify_hooks_resource_update,
                #    [resource, webhook, website]
                #)
            
            #resource/document delete
            elif operation == DomainObjectOperation.deleted:
                topic = 'resource/delete'

                resource = table_dictize(entity, context)

                #tasks.notify_hooks_resource_delete(resource, webhook, website)

                #jobs.enqueue(
                #    tasks.notify_hooks_resource_update,
                #    [resource, webhook, website]
                #)
                
            else:
                return

        if isinstance(entity, model.Package):

            #Dataset create
            if operation == DomainObjectOperation.new:
                topic = 'dataset/create'
                resource = table_dictize(entity, context)

                #tasks.notify_hooks_dataset_create(resource)
                    
                #jobs.enqueue(
                #    tasks.notify_hooks_dataset_create,
                #    [resource, webhook, website]
                #)

            #Dataset update
            #Most likely not required as falkor doesnt allow updating datasets
            elif operation == DomainObjectOperation.changed:
                topic = 'dataset/update'

            #Dataset delete
            #Most likely not required as falkor doesnt allow deleting datasets
            elif operation == DomainObjectOperation.deleted:
                topic = 'dataset/delete'

            else:
                return
