# RTDI Big Data FileConnector 

_Constantly scan a given directory for new files matching a name pattern. If a new file appears, it is automatically parsed, mapped to the target structure, loaded into Kafka and then renamed to .processed. For configuring the file format definitions, the connector has a data driven UI._

Source code available here: [github](https://github.com/rtdi/RTDIFileConnector)

## Design Thinking goal
* As a business user I would like to load comma separated files into Apache Kafka, one line as one message
* UIs are used to configure all
* Both schema options are needed, the ability to populate a schema matching the file structure and the ability to map the file structure to an existing server schema

## Core thoughts

Comma separated files (CSV files) exist in different variations and hence require a lot of settings, many not immediately obvious. For example the file might originate from a system with a different character encoding set, might use country dependent formatting, escape characters, unusual column and row separators and many more. For such a scenario a data driven UI mstches the user needs best. A file is opened and step by step refined until its entire structure is defined, preferably with a lot of automation in the format detection.

## Installation and testing

On any computer install the Docker Daemon - if it is not already - and download this docker image with

    docker pull rtdi/fileconnector

Then start the image via docker run. For a quick test this command is sufficient

    docker run -d -p 80:8080 --name fileconnector  rtdi/fileconnector

to expose a webserver at port 80 on the host running the container. Make sure to open the web page via the http prefix, as https needs more configuration.
For example [http://localhost:80/](http://localhost:80/) might do the trick of the container is hosted on the same computer.

The default login for this startup method is: **rtdi / rtdi!io**

For proper start commands see the [ConnectorRootApp](https://github.com/rtdi/ConnectorRootApp) project, this application is using as foundation.

## Help!

The source code of this project is available at [github](https://github.com/rtdi/RTDIFileConnector).
As an OpenSource project it grows with the interactions. Hence I invite all to create [issues](https://github.com/rtdi/RTDIFileConnector/issues) in github, no matter if it is a request for help or a product suggestions. Also, please spread the word. The more people are using it, the faster progress will be made to your benefit.


## Capabilities

The complete solution consists of the following modules:

* Connect to an existing Apache Kafka server or a hosted Kafka service like Confluent Cloud
* Define the file format setting via a UI; settings are stored as annotated AVRO schema files.
* Optionally map the file format to an existing schema
* Constantly scan for files in a given directory, parse them and send each line as one message. One file is one Kafka transaction.

<img src="https://github.com/rtdi/RTDIFileConnector/raw/master/docs/media/FileConnector-Homepage.png" width="50%">

### Connect the Pipeline to Kafka

The first step is to connect the application to a Kafka server, in this example Confluent Cloud.

<img src="https://github.com/rtdi/RTDIFileConnector/raw/master/docs/media/FileConnector-PipelineConfig.png" width="50%">


### Define a Connection

A Connection represents a directory with the data files. Within one directory there can be many files, even with different formats.

<img src="https://github.com/rtdi/RTDIFileConnector/raw/master/docs/media/FileConnector-Connection1.png" width="50%">


### Define the file format

Each connection can have multiple file formats - schemas - defined. 

<img src="https://github.com/rtdi/RTDIFileConnector/raw/master/docs/media/FileConnector-Connection1-overview.png" width="50%">

When creating a new schema, the first screen defines the file global settings. A file format has a name, it matches certain file name patterns (in regular expression format), it has a character encoding and a language default.
To help finding the proper values, if a file is found its contents will be shown as text information. This helps to set e.g. the correct character set.

Note: Important characters in the [regular expression syntax](https://www.freeformatter.com/java-regex-tester.html) are
- .* matching any character 0..n times
- \. means a dot character by itself

Example: uscensus.*\.csv matches all files that start with the text "uscensus" and have the prefix ".csv", e.g. uscensus_2010.csv would be found.

<img src="https://github.com/rtdi/RTDIFileConnector/raw/master/docs/media/FileConnector-Format-Def1.png" width="50%">


In the next tab the parsing information is defined. What is the line delimiter, the column separator, does the file have a header row, the data types for each column. To speed up the process the format can be guessed as well and then further refined.

<img src="https://github.com/rtdi/RTDIFileConnector/raw/master/docs/media/FileConnector-Format-Def2.png" width="50%">

The column definition tab is for the details about each column, primarily the format strings to be used when parsing e.g. date value.

<img src="https://github.com/rtdi/RTDIFileConnector/raw/master/docs/media/FileConnector-Format-Def3.png" width="50%">


### Create a Producer

A Producer stands for the process scanning for files matching a schema, reading the files and the lines and sending the data to a server topic.

<img src="https://github.com/rtdi/RTDIFileConnector/raw/master/docs/media/FileConnector-Add-Producer.png" width="50%">

<img src="https://github.com/rtdi/RTDIFileConnector/raw/master/docs/media/FileConnector-Producer.png" width="50%">


### Data content

As the schema was not mapped to any server schema, the Producer automatically create a schema on the server.
The payload contains all the columns plus some extra metadata about the file/row. One file is loaded as one Kafka transaction and renamed after processing to either .error or .processed.

<img src="https://github.com/rtdi/RTDIFileConnector/raw/master/docs/media/FileConnector-Data.png" width="50%">


### Extension points

To extend this application and use it beyond the editing capabilities, own code can be added via various ways.
- The directory /usr/local/tomcat/conf/rtdiconfig/ contains all settings in the form of json files. Might be a good idea to hook up a host directory into this location.
- As it is based on docker, any customizations docker allows can be applied
- The base image is a tomcat webserver, which allows adding more web applications and files

## Licensing

This application is provided as dual license. For all users with less than 100'000 messages created per month, the application can be used free of charge and the code falls under a Gnu Public License. Users with more than 100'000 messages are asked to get a commercial license to support further development of this solution. The commercial license is on a monthly pay-per-use basis.


## Data protection and privacy

Every ten minutes the application does send the message statistics via a http call to a central server where the data is stored for information along with the public IP address (usually the IP address of the router). It is just a count which service was invoked how often, no information about endpoints, users, data or URL parameters. This information is collected to get an idea about the adoption.
To disable that, set the environment variable HANAAPPCONTAINERSTATISTICS=FALSE.