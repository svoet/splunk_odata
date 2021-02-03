#from __future__ import print_function
#from future import standard_library
#standard_library.install_aliases()
from builtins import str
import sys
import logging
import pyodata
import requests
import pdb
import json
#import splunk.entity as entity


#set up logging suitable for splunkd comsumption
logging.root
logging.root.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(levelname)s %(message)s')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logging.root.addHandler(handler)

SCHEME = """<scheme>
    <title>Twitter</title>
    <description>Get data from OData service.</description>
    <use_external_validation>true</use_external_validation>
    <streaming_mode>simple</streaming_mode>
    <endpoint>
        <args>
            <arg name="name">
                <title>OData service name</title>
                <description>Name of the OData service.</description>
            </arg>
            <arg name="username">
                <title>Username</title>
                <description>Username for the service.</description>
            </arg>
            <arg name="password">
                <title>Password</title>
                <description>Password for the service</description>
            </arg>
            <arg name="entity_sets">
                <title>Entity sets</title>
                <description>Comma-separated list of entity sets, eg 'Employees, Customers'</description>
            </arg>
            <arg name="filter">
                <title>Filter</title>
                <description>Filter to apply on the request. eg "FirstName eq 'John' and LastName eq 'Smith'" </description>
            </arg>
            <arg name="attributes">
                <title>Attributes</title>
                <description>Comma-seperated list of attributes to fetch from returned objects. eg "EmployeeID" </description>
            </arg>
        </args>
    </endpoint>
</scheme>
"""

def do_scheme():
	print(SCHEME)

# prints XML error data to be consumed by Splunk
def print_error(s):
    print("<error><message>%s</message></error>" % xml.sax.saxutils.escape(s))

def validate_conf(config, key):
    if key not in config:
        raise Exception("Invalid configuration received from Splunk: key '%s' is missing." % key)

#read XML configuration passed from splunkd
def get_config():
    config = {}

    try:
        # read everything from stdin
        config_str = sys.stdin.read()

        # parse the config XML
        doc = xml.dom.minidom.parseString(config_str)
        root = doc.documentElement
        conf_node = root.getElementsByTagName("configuration")[0]
        if conf_node:
            logging.debug("XML: found configuration")
            stanza = conf_node.getElementsByTagName("stanza")[0]
            if stanza:
                stanza_name = stanza.getAttribute("name")
                if stanza_name:
                    logging.debug("XML: found stanza " + stanza_name)
                    config["name"] = stanza_name

                    params = stanza.getElementsByTagName("param")
                    for param in params:
                        param_name = param.getAttribute("name")
                        logging.debug("XML: found param '%s'" % param_name)
                        if param_name and param.firstChild and \
                           param.firstChild.nodeType == param.firstChild.TEXT_NODE:
                            data = param.firstChild.data
                            config[param_name] = data
                            logging.debug("XML: '%s' -> '%s'" % (param_name, data))

        checkpnt_node = root.getElementsByTagName("checkpoint_dir")[0]
        if checkpnt_node and checkpnt_node.firstChild and \
           checkpnt_node.firstChild.nodeType == checkpnt_node.firstChild.TEXT_NODE:
            config["checkpoint_dir"] = checkpnt_node.firstChild.data

        if not config:
            raise Exception("Invalid configuration received from Splunk.")

        # just some validation: make sure these keys are present (required)
        validate_conf(config, "name")
        validate_conf(config, "username")
        validate_conf(config, "password")
        validate_conf(config, "checkpoint_dir")
        validate_conf(config, "service_url")
    except Exception as e:
        raise Exception("Error getting Splunk configuration via STDIN: %s" % str(e))

    return config

def get_validation_data():
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

def connect(service_url,username,password):
    session = requests.Session()
    if len(username) > 0:
        session.auth = (username, password)

    return pyodata.Client(service_url, session)

def run(config=None):
    if not config:
        config =get_config()

    username=config["username"] if "username" in config else ""
    password=config["password"] if "password" in config else ""
    service_url=config["service_url"] if "service_url" in config else ""
    entity_sets=config["entity_sets"] if "entity_sets" in config else ""
    attributes=config["attributes"] if "attributes" in config else ""
    filter=config["filter"] if "filter" in config else ""

    # Validate username and password before starting splunk listener.
    logging.debug("Credentials found: username = %s, password = %s" %(username,password))
    service=connect(service_url,username,password)

    result=do_query(service,entity_sets,attributes,filter)

def do_query(service,entity_sets,attributes,filter):
    for name,es in service.entity_sets._entity_sets.items():
        if len(entity_sets) > 0 and name not in entity_sets:
            continue

        request = es.get_entities()
        if len(filter) > 0:
            request=request.filter(filter)

        for entity in request.execute():
            res=get_entity(entity,attributes,name)
            print(json.dumps(res))

def get_entity(entity, attributes,name):
    #res={'entity_type':name,'entity_key':entity.entity_key.to_key_string_without_parentheses()}
    res={'entity_type':name}
    #pdb.set_trace()
    for property in entity.entity_set.entity_type.proprties():
        if len(attributes) > 0 and property.name not in attributes:
            continue
        value=getattr(entity,property.name)
        #recursive lookup of object
        if value.__class__.__name__ == 'EntityProxy':
            if not value.entity_set:
                continue
            value=get_entity(value,attributes,property.name)
        if value.__class__.__name__ == 'datetime':
            value=value.isoformat()
        res.update({property.name:value})
    return res

if __name__ == '__main__':
    if len(sys.argv ) > 1:
        if sys.argv[1] == "--scheme":
            do_scheme()
        elif sys.argv[1] == "--validate-arguments":
            if len(sys.argv)>3:
                validate_config(sys.argv[2],sys.argv[3])
            else:
                print('supply username and password')
        elif sys.argv[1] == "--test":
            config={ 'service_url':'http://services.odata.org/V2/Northwind/Northwind.svc/'}
            run(config)
        elif sys.argv[1] == "--test-filtered":
            print("Get multiple entity sets")
            config={ 'service_url':'http://services.odata.org/V2/Northwind/Northwind.svc/',
                    'entity_sets':"Employees,Customers"}
            run(config)
            print("Get filtered entity set")
            config={ 'service_url':'http://services.odata.org/V2/Northwind/Northwind.svc/',
                    'entity_sets':"Employees",
                    'filter':"FirstName eq 'Nancy' and LastName eq 'Davolio'"}
            run(config)
            print("Get only specific attributs")
            config={ 'service_url':'http://services.odata.org/V2/Northwind/Northwind.svc/',
                    'entity_sets':"Employees",
                    'attributes':"FirstName,LastName"}
            run(config)
        else:
            print('You giveth weird arguments')
    else:
        # just request data from Twitter
        run()

    sys.exit(0)


