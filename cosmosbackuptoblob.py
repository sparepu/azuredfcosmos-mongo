from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
from datetime import datetime, timedelta
import time
import argparse
import sys
from azure.keyvault import KeyVaultClient
from azure.common.credentials import ServicePrincipalCredentials

def getOptions(args=sys.argv[1:]):
    parser = argparse.ArgumentParser(description="Parses command.")
    parser.add_argument("-s", "--subscription",  help="Azure subscription id", required=True)
    parser.add_argument("-g", "--resourcegroup", help="Azure Resource Group Name", required=True)
    parser.add_argument("-d", "--datafactory", help="Azure DataFactory Name either to create or update", required=True)
    parser.add_argument("-clientid", "--clientid", help="Service Principal Client ID", required=True)
    parser.add_argument("-clientsecret", "--clientsecret", help="Service Principal Client Secret", required=True)
    parser.add_argument("-tenantid", "--tenantid", help="Azure Tenant ID", required=True)
    parser.add_argument("-irid", "--integrationruntime", help="Data factory Integration Runtime", required=False)
    parser.add_argument("-vault", "--vaulturl", help="Azure Key Vault URL in https://<>.vault.azure.net format", required=True)
    parser.add_argument("-sourcesecret", "--secretnameforsourceconnectionstring", help="Secret Name for source connection string", required=True)
    parser.add_argument("-sinksecret", "--secretnameforsinkconnectionstring", help="Secret Name for Storage connection string", required=True)
    parser.add_argument("-sourcedb", "--sourcedatabasename", help="Database Name in source cosmos", required=True)
    parser.add_argument("-sourcecollection", "--sourcecollectionname", help="Collection Name in source cosmos", required=True)
    parser.add_argument("-sinkblob", "--sinkcontainername", help="Container Name in sink storage", required=True)
    parser.add_argument("-incr", "--incremental", help="Set to 'yes' for incremental backup", required=False)
    options = parser.parse_args(args)
    return options
options = getOptions(sys.argv[1:])


def print_item(group):
    """Print an Azure object instance."""
    print("\tName: {}".format(group.name))
    print("\tId: {}".format(group.id))
    if hasattr(group, 'location'):
        print("\tLocation: {}".format(group.location))
    if hasattr(group, 'tags'):
        print("\tTags: {}".format(group.tags))
    if hasattr(group, 'properties'):
        print_properties(group.properties)

def print_properties(props):
    """Print a ResourceGroup properties instance."""
    if props and hasattr(props, 'provisioning_state') and props.provisioning_state:
        print("\tProperties:")
        print("\t\tProvisioning State: {}".format(props.provisioning_state))
    print("\n\n")

def print_activity_run_details(activity_run):
    """Print activity run details."""
    print("\n\tActivity run details\n")
    print("\tActivity run status: {}".format(activity_run.status))
    if activity_run.status == 'Succeeded':
        print("\tNumber of bytes read: {}".format(activity_run.output['dataRead']))
        print("\tNumber of bytes written: {}".format(activity_run.output['dataWritten']))
        print("\tCopy duration: {}".format(activity_run.output['copyDuration']))
    else:
        print("\tErrors: {}".format(activity_run.error['message']))


def azurekeyvault():

    credentials = ServicePrincipalCredentials(client_id=options.clientid, secret=options.clientsecret, tenant=options.tenantid)
    client = KeyVaultClient(credentials)
    vault_url = options.vaulturl
    sourcesecret = options.secretnameforsourceconnectionstring
    sinksecret = options.secretnameforsinkconnectionstring
    source_bundle = client.get_secret(vault_url, sourcesecret, "")
    sink_bundle = client.get_secret(vault_url, sinksecret, "")
    sourceconnectionstring = source_bundle.value
    sinkconnectionstring = sink_bundle.value
    return sourceconnectionstring, sinkconnectionstring

def datafactory(sourceconnectionstring, sinkconnectionstring):


    # Azure subscription ID
    subscription_id = options.subscription

    # This program creates this resource group. If it's an existing resource group, comment out the code that creates the resource group
    rg_name = options.resourcegroup

    # The data factory name. It must be globally unique.
    df_name = options.datafactory

    # Specify your Active Directory client ID, client secret, and tenant ID
    credentials = ServicePrincipalCredentials(client_id=options.clientid, secret=options.clientsecret, tenant=options.tenantid)
    resource_client = ResourceManagementClient(credentials, subscription_id)
    adf_client = DataFactoryManagementClient(credentials, subscription_id)

    rg_params = {'location':'westeurope'}
    df_params = {'location':'westeurope'}

    #Create a data factory
    df_resource = Factory(location='westeurope')
    df = adf_client.factories.create_or_update(rg_name, df_name, df_resource)
    print_item(df)
    while df.provisioning_state != 'Succeeded':
        df = adf_client.factories.get(rg_name, df_name)
        time.sleep(1)
 
    if options.integrationruntime is not None: 
       integrationruntime = IntegrationRuntimeReference(reference_name=options.integrationruntime, parameters=None)

    
    source_ls_name = 'sourceLinkedService'
    if options.integrationruntime is not None:       
       source_ls_azure_cosmos = CosmosDbMongoDbApiLinkedService(connection_string=sourceconnectionstring, database=options.sourcedatabasename, connect_via=integrationruntime)
    else:
       source_ls_azure_cosmos = CosmosDbMongoDbApiLinkedService(connection_string=sourceconnectionstring, database=options.sourcedatabasename)
    source_ls = adf_client.linked_services.create_or_update(rg_name, df_name, source_ls_name, source_ls_azure_cosmos)
    print_item(source_ls)
  
    # Create an Azure blob dataset (input)
    source_ds_name = 'sourceDS'
    source_ds_ls = LinkedServiceReference(reference_name=source_ls_name)
    collection = options.sourcecollectionname
    sourcedataset = CosmosDbMongoDbApiCollectionDataset(linked_service_name=source_ds_ls, collection= collection, schema=None)
    source_ds = adf_client.datasets.create_or_update(rg_name, df_name, source_ds_name, sourcedataset)
    print_item(source_ds)

    sink_ls_name = 'sinkLinkedService'
    if options.integrationruntime is not None:
       sink_ls_azure_blob = AzureStorageLinkedService(connection_string=sinkconnectionstring, connect_via=integrationruntime)
    else:
       sink_ls_azure_blob = CosmosDbMongoDbApiLinkedService(connection_string=sinkconnectionstring)
    sink_ls = adf_client.linked_services.create_or_update(rg_name, df_name, sink_ls_name, sink_ls_azure_blob)
    print_item(sink_ls)

    sink_ds_name = 'sinkDS'
    utc_datetime = datetime.utcnow()
    blob_filename = utc_datetime.strftime("%Y%m%d-%H%M%SZ") + '.json.gz'
    sink_ds_ls = LinkedServiceReference(reference_name=sink_ls_name)
    location = DatasetLocation(type='AzureBlobStorageLocation',folder_path=options.sinkcontainername,file_name=blob_filename)
    compression = DatasetGZipCompression(level='Optimal')
    sinkdataset = JsonDataset(linked_service_name=sink_ds_ls, compression=compression, location=location)
    sink_ds = adf_client.datasets.create_or_update(rg_name, df_name, sink_ds_name, sinkdataset)
    print_item(sink_ds)

    if options.incremental is not None:
       if options.incremental == "yes":
          # Create a copy activity
          currentime = datetime.now()
          currentobjectid = 'ObjectId' + '(' + '"' + format(int(time.mktime(currentime.timetuple())), 'x') + "0000000000000000" + '"' +  ')'
          previoustime = (datetime.now() - timedelta(minutes=240))
          previousobjectid = 'ObjectId' + '(' + '"' + format(int(time.mktime(previoustime.timetuple())), 'x') + "0000000000000000" + '"' +  ')'
          filter = '{_id: {$gte:'+ previousobjectid + ',$lt:' + currentobjectid + '}}'

    act_name = 'copyCosmostoblob'
    if options.incremental is not None:
       if options.incremental == "yes":
          cosmos_source = CosmosDbMongoDbApiSource(filter=filter)
       else:
          print ("Set incremental to yes for this script to work properly")
          sys.exit (1) 
    else:
       cosmos_source = CosmosDbMongoDbApiSource()
    blob_sink = BlobSink()
    dsin_ref = DatasetReference(reference_name=source_ds_name)
    dsOut_ref = DatasetReference(reference_name=sink_ds_name)
    copy_activity = CopyActivity(name=act_name,inputs=[dsin_ref], outputs=[dsOut_ref], source=cosmos_source, sink=blob_sink)

    #Create a pipeline with the copy activity
    p_name = 'copyPipeline'
    params_for_pipeline = {}
    p_obj = PipelineResource(activities=[copy_activity], parameters=params_for_pipeline)
    p = adf_client.pipelines.create_or_update(rg_name, df_name, p_name, p_obj)
    print_item(p)

    run_response = adf_client.pipelines.create_run(rg_name, df_name, p_name, parameters={})

    time.sleep(30)
    pipeline_run = adf_client.pipeline_runs.get(rg_name, df_name, run_response.run_id)
    print("\n\tPipeline run status: {}".format(pipeline_run.status))
    filter_params = RunFilterParameters(
       last_updated_after=datetime.now() - timedelta(1), last_updated_before=datetime.now() + timedelta(1))
    query_response = adf_client.activity_runs.query_by_pipeline_run(
       rg_name, df_name, pipeline_run.run_id, filter_params)
    print_activity_run_details(query_response.value[0])


# Start the main method
def main():
     
   cs = azurekeyvault()
   datafactory(cs[0], cs[1])
   
main()
