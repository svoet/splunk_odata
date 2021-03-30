[odata://<name>]
*Get data from OData source

service_url = <string> URL of the OData endpoint, eg 'http://services.odata.org/V2/Northwind/Northwind.svc/'
username = <value> This is the user's twitter username/handle
password = <password> This is the user's password used for logging into twitter
entity_sets = <list> Comma-separated list of entity sets, eg 'Employees, Customers'
* Optional
* If entity_sets field is left empty, ALL entity sets will be retrieved
attributes = <list> Comma-seperated list of attributes to fetch from returned objects. eg "EmployeeID"
* Optional
* If attributes field is left empty, ALL attributes will be retrieved
filter = <string> Filter to apply on the request. eg "FirstName eq 'John' and LastName eq 'Smith'"
* Optional
* If filter is left empty, no filtering is applied

* You may combine entity_sets and attributes filtering. Attributes missing on one of the entity sets will be simply omitted
* You may combine entity_sets with filters, but be aware that the filter must be able to apply to all selected entity_sets

