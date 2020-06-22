# RTDI Big Data FileConnector 

Scans a given directory and file name pattern for new files. If a new file appears, it is automatically parsed, mapped to the target structure, loaded into Kafka and then renamed to .processed.

For configuring the file format definitions, the connector has a data driven UI.

# Default Login

The default login is: **rtdi / rtdi!io**

For production use this must be changed, see [ConnectorRootApp](https://github.com/rtdi/ConnectorRootApp) for technical setup details.

