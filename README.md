There are two scripts created for this task.

**cosmos to cosmos data movement**

This is required for changing server names or for refreshing staging with prod data.

Usage can be checked using -h

```
python3 cosmosreplication.py -h
usage: cosmosreplication.py [-h] -s SUBSCRIPTION -g RESOURCEGROUP -d
.......
```


Script should be run as below

```
python3 cosmosreplication.py -s <AzureSubscriptionID> -g <ResourceGroup> -d <DatafactoryName> -clientid <ServiceprincipalID> -clientsecret <ServiceprincipalSecret> -tenantid <AzureTenantID> -ir <IntegrationRuntimeName> -vault <AzurekeyvaultURL> -sourcesecret <Sourceconnectionstring> -sinksecret <Sinkconnectionstring> -sourcedb <Sourcedatabasename> -sinkdb <Sinkdatabasename> -sourcecollection <Sourcecollectiontocopy> -sinkcollection <Sinkcollectiontocopyto> -incr yes
```

Pre-requisites for running the script.

Integration run time (self-hosted) one should be setup if we are dealing with private datasets.
Copy connection strings (Copy entire connection string that is displayed in Azure Portal or CLI) to keystore for both input and output datasets.
What does script do.

      1. Script fetch connection strings from keyvault. (specified in script arguments). 
      2. It creates datafactory service if not present. This operation is idempotent. We all can use same datafactory for our team for multiple pipelines.
      3. It creates linked service, datasets, copyactivities, pipeline
      4. By default, it copies full dataset from source to destination. In case, if you do not want full backup, please specify -incr yes This forces script to perform only incremental backup (Keep in mind, the data that is changed in last 4 hours is copied to destination). This is used for scheduling weekly full backups and incremental backups. The time can be modified as per our requirement.


**ComosDB to Azure Storage**

```
python3 cosmosbackuptoblob.py  s <AzureSubscriptionID> -g <ResourceGroup> -d <DatafactoryName> -clientid <ServiceprincipalID> -clientsecret <ServiceprincipalSecret> -tenantid <AzureTenantID> -ir <IntegrationRuntimeName> -vault <AzurekeyvaultURL> -sourcesecret <Sourceconnectionstring> -sinksecret <Sinkconnectionstring> -sourcedb <Sourcedatabasename>  -sourcecollection <Sourcecollectiontocopy> -sinkblob <BlobonAzureStorage> -incr yes
```

This script also does the same except that it copies to Azure blob that is required for our daily backups.

Speed of operation is roughly 90MB per sec in production environments. Make sure that you use secondary mongodb server for this task as primary mongodb will be serving production operations. 
