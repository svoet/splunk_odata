###!/usr/bin/env python3
#from __future__ import print_function
#from future import standard_library
#standard_library.install_aliases()
from builtins import str
import sys
import logging
import os

sys.path.insert(0, os.path.sep.join([os.path.dirname(os.path.realpath(os.path.dirname(__file__))), 'lib']))
if sys.version_info < (3, 0):
    #sys.path.insert(0, os.path.sep.join([os.path.dirname(os.path.realpath(os.path.dirname(__file__))), 'lib', 'py2']))
    raise Exception("Python version {} not supported".format(sys.version_info))
    exit
else:
    sys.path.append('/usr/local/lib/python3.6/dist-packages/') 
    sys.path.insert(0, os.path.sep.join([os.path.dirname(os.path.realpath(os.path.dirname(__file__))), 'lib', 'py3']))
    sys.path.insert(0, os.path.sep.join([os.path.dirname(os.path.realpath(os.path.dirname(__file__))), 'lib']))
import pyodata
import requests
import pdb
import json
#import splunk.entity as entity
import splunklib.client as client
from splunklib.modularinput import *

#set up logging suitable for splunkd comsumption
logging.root
logging.root.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(levelname)s %(message)s')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logging.root.addHandler(handler)


class OData(Script):
    _MASK = '******'
    def get_scheme(self):
        scheme = Scheme("OData API input")
        scheme.description = ("Get data from OData service.")
        scheme.use_external_validation = True
        scheme.streaming_mode_xml = True #was: simple
        scheme.use_single_instance = True

        username_arg = Argument(
            name="username",
            title="User name",
            data_type=Argument.data_type_string,
            required_on_create=True,
            required_on_edit=True
        )
        scheme.add_argument(username_arg)
        
        password_arg = Argument(
            name="password",
            title="Password",
            data_type=Argument.data_type_string,
            required_on_create=True,
            required_on_edit=True
        )
        scheme.add_argument(password_arg)

        name_arg = Argument(
            name="name",
            title="OData service name",
            data_type=Argument.data_type_string,
            required_on_create=False,
            required_on_edit=False
        )
        scheme.add_argument(name_arg)

        service_url_arg = Argument(
            name="service_url",
            title="OData service url",
            data_type=Argument.data_type_string,
            required_on_create=False,
            required_on_edit=False
        )
        scheme.add_argument(service_url_arg)

        entity_sets_arg = Argument(
            name="entity_sets",
            title="Comma-separated list of entity sets, eg 'Employees, Customers'. All entity sets are obtained when field left empty",
            data_type=Argument.data_type_string,
            required_on_create=False,
            required_on_edit=False
        )
        scheme.add_argument(entity_sets_arg)

        attributes_arg = Argument(
            name="attributes",
            title="Comma-seperated list of attributes to fetch from returned objects. eg \"EmployeeID\" . When field left empty, all attributes will be obtained",
            data_type=Argument.data_type_string,
            required_on_create=False,
            required_on_edit=False
        )
        scheme.add_argument(attributes_arg)

        filter_arg = Argument(
            name="filter",
            title='Filter to apply on the request. eg "FirstName eq \'John\' and LastName eq \'Smith\'" . When field left empty, no filter will be applied',
            data_type=Argument.data_type_string,
            required_on_create=False,
            required_on_edit=False
        )
        scheme.add_argument(filter_arg)
        return scheme

    #read XML configuration passed from splunkd
    def check_config(self,config):

        #Store masked password / obtain clear password
        session_key = self._input_definition.metadata["session_key"]
        try:
            # If the password is not masked, mask it.
            if config['password'] != self._MASK:
                self.encrypt_password(config['username'], config['password'], session_key)
                self.mask_password(session_key, config['username'])

            config['password'] = self.get_password(session_key, config['username'])
            # just some validation: make sure these keys are present (required)
            self.validate_conf(config, "username")
            self.validate_conf(config, "password")
            #self.validate_conf(config, "checkpoint_dir")
            self.validate_conf(config, "service_url")
        except Exception as e:
            raise Exception("Error getting Splunk configuration via STDIN: %s" % str(e))

        return config

    ###############PASSWORD ENCRYPTION###############
    def encrypt_password(self, username, password, session_key):
        args = {'token':session_key}
        service = client.connect(**args)
        
        try:
            # If the credential already exists, delte it.
            for storage_password in service.storage_passwords:
                if storage_password.username == username:
                    service.storage_passwords.delete(username=storage_password.username)
                    break

            # Create the credential.
            service.storage_passwords.create(password, username)

        except Exception as e:
            raise Exception ("An error occurred updating credentials. Please ensure your user account has admin_all_objects and/or list_storage_passwords capabilities. Details: %s".format(str(e)))

    def mask_password(self, session_key, username):
        try:
            args = {'token':session_key}
            service = client.connect(**args)
            kind, input_name = self.input_name.split("://")
            item = service.inputs.__getitem__((input_name, kind))
            
            kwargs = {
                "username": username,
                "password": self._MASK
            }
            item.update(**kwargs).refresh()
            
        except Exception as e:
            raise Exception("Error updating inputs.conf: {}".format(str(e)))

    def get_password(self, session_key, username):
        args = {'token':session_key}
        service = client.connect(**args)

        # Retrieve the password from the storage/passwords endpoint	
        for storage_password in service.storage_passwords:
            if storage_password.username == username:
                return storage_password.content.clear_password

    # prints XML error data to be consumed by Splunk
    def print_error(self, s):
        print("<error><message>{}</message></error>".format(xml.sax.saxutils.escape(s)))

    def validate_conf(self, config, key):
        if key not in config:
            raise Exception("Invalid configuration received from Splunk: key '{}' is missing.".format( key)) 
    def get_validation_data(self):
        val_data = {}

        # read everything from stdin
        val_str = sys.stdin.read()

        # parse the validation XML
        doc = xml.dom.minidom.parseString(val_str)
        root = doc.documentElement

        logging.debug("XML: found items")
        item_node = root.getElementsByTagName("item")[0]
        if item_node:
            logging.debug("XML: found item")

            name = item_node.getAttribute("name")
            val_data["stanza"] = name

            params_node = item_node.getElementsByTagName("param")
            for param in params_node:
                name = param.getAttribute("name")
                logging.debug("Found param %s" % name)
                if name and param.firstChild and \
                   param.firstChild.nodeType == param.firstChild.TEXT_NODE:
                    val_data[name] = param.firstChild.data

        return val_data

    def connect(self,service_url,username,password):
        session = requests.Session()
        session.verify='/etc/ssl/certs'
        if len(username) > 0:
            session.auth = (username, password)

        return pyodata.Client(service_url, session)

    def stream_events(self,inputs,ew):
        ew.log("INFO","inputs: {}".format(inputs.inputs.items()))
        self.input_name,input_item = inputs.inputs.popitem()
        config =self.check_config(input_item)

        username=config["username"] if "username" in config else ""
        password=config["password"] if "password" in config else ""
        service_url=config["service_url"] if "service_url" in config else ""
        entity_sets=config["entity_sets"] if "entity_sets" in config else ""
        attributes=config["attributes"] if "attributes" in config else ""
        filter=config["filter"] if "filter" in config else ""

        # Validate username and password before starting splunk listener.
        logging.debug("Credentials found: username = %s, password = %s" %(username,password))
        service=self.connect(service_url,username,password)

        self.do_query(service,ew,entity_sets,attributes,filter)

    def do_query(self,service,ew,entity_sets,attributes,filter):
        for name,es in service.entity_sets._entity_sets.items():
            if len(entity_sets) > 0 and name not in entity_sets:
                continue

            request = es.get_entities()
            if len(filter) > 0:
                request=request.filter(filter)

            for entity in request.execute():
                res=self.get_entity(entity,attributes,name)
                event = Event()
                event.stanza = self.input_name
                event.data = json.dumps(res)
                ew.write_event(event)

    def get_entity(self,entity, attributes,name):
        res={'entity_type':name}
        for property in entity.entity_set.entity_type.proprties():
            if len(attributes) > 0 and property.name not in attributes:
                continue
            value=getattr(entity,property.name)
            #recursive lookup of object
            if value.__class__.__name__ == 'EntityProxy':
                if not value.entity_set:
                    continue
                value=self.get_entity(value,attributes,property.name)
            if value.__class__.__name__ == 'datetime':
                value=value.isoformat()
            res.update({property.name:value})
        return res

if __name__ == '__main__':
    if len(sys.argv ) > 1:
        if sys.argv[1] == "--test":
            config={ 'service_url':'http://services.odata.org/V2/Northwind/Northwind.svc/'}
            OData().run(config)
            sys.exit(0)
        elif sys.argv[1] == "--test-filtered":
            print("Get multiple entity sets")
            config={ 'service_url':'http://services.odata.org/V2/Northwind/Northwind.svc/',
                    'entity_sets':"Employees,Customers"}
            OData().run(config)
            print("Get filtered entity set")
            config={ 'service_url':'http://services.odata.org/V2/Northwind/Northwind.svc/',
                    'entity_sets':"Employees",
                    'filter':"FirstName eq 'Nancy' and LastName eq 'Davolio'"}
            OData().run(config)
            print("Get only specific attributs")
            config={ 'service_url':'http://services.odata.org/V2/Northwind/Northwind.svc/',
                    'entity_sets':"Employees",
                    'attributes':"FirstName,LastName"}
            OData().run(config)
            sys.exit(0)

    exitcode = OData().run(sys.argv)
    sys.exit(exitcode)


