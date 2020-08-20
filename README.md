## oppdrag-faktura
A standalone Elasticsearch application that harvests invoices from Redmine CRM through HTTP transport and index them into Elasticsearch. This application is too specific to the University of Bergen Library but by looking at the source codes, you might get a hint on how to write an application that can communicate with Elasticsearch through HTTP. 


### How to run the application

- Download the latest release from https://github.com/ubbdst/oppdrag-faktura/releases. The file contains all dependencies of the project. Hence when extracting, you must keep all files (dependencies) in the same folder.

- Go to a command line and type <code> java -jar -DapiKey="hakuna-matata" -Dcluster="elasticsearch" -Dhost="kiri.uib.no" /var/lib/oppdrag/oppdrag-faktura-1.0-SNAPSHOT.jar</code>
The application will look for the system variable <code>apiKey</code> for authentication, if not provided, default key will be used. Please follow the Redmine documentation for how to get your API key here http://www.redmine.org/projects/redmine/wiki/Rest_api 
- <code>cluster, host, apiKey, index, type </code> can be passed as parameters. 

- After successful authentication, the application will harvest all invoices from https://oppdrag.ub.uib.no and join the available Elasticsearch cluster. By default, all invoices will then be indexed into Elasticsearch with index <code>admin</code> and type <code>invoice</code>. 

- You may then search for documents at <code>http://localhost:9200/admin/invoice/_search?pretty</code>
