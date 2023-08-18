// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using Azure;
using Azure.Core;
using Azure.Identity;
using Azure.ResourceManager.Resources.Models;
using Azure.ResourceManager.Samples.Common;
using Azure.ResourceManager.Resources;
using Azure.ResourceManager;
using Azure.ResourceManager.CosmosDB.Models;
using Azure.ResourceManager.CosmosDB;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents;

namespace CosmosDBWithEventualConsistency
{

    public class Program
    {
        private static ResourceIdentifier? _resourceGroupId = null;
        private const int _maxStalenessPrefix = 100000;
        private const int _maxIntervalInSeconds = 300;
        const String DATABASE_ID = "TestDB";
        const String COLLECTION_ID = "TestCollection";

        /**
          * Azure CosmosDB sample -
          *  - Create a CosmosDB configured with eventual consistency
          *  - Get the credentials for the CosmosDB
          *  - add collection to the CosmosDB
          *  - Delete the CosmosDB.
          */
        public static async Task RunSample(ArmClient client)
        {
            try
            {
                // Get default subscription
                SubscriptionResource subscription = await client.GetDefaultSubscriptionAsync();

                // Create a resource group in the EastUS region
                string rgName = Utilities.CreateRandomName("CosmosDBTemplateRG");
                Utilities.Log($"Creating a resource group..");
                ArmOperation<ResourceGroupResource> rgLro = await subscription.GetResourceGroups().CreateOrUpdateAsync(WaitUntil.Completed, rgName, new ResourceGroupData(AzureLocation.EastUS));
                ResourceGroupResource resourceGroup = rgLro.Value;
                _resourceGroupId = resourceGroup.Id;
                Utilities.Log("Created a resource group with name: " + resourceGroup.Data.Name);

                //============================================================
                // Create a CosmosDB.

                Utilities.Log("Creating a CosmosDB...");
                string dbAccountName = Utilities.CreateRandomName("dbaccount");
                CosmosDBAccountKind cosmosDBKind = CosmosDBAccountKind.GlobalDocumentDB;
                var locations = new List<CosmosDBAccountLocation>()
                {
                    new CosmosDBAccountLocation(){ LocationName  = AzureLocation.WestUS, FailoverPriority = 0 },
                    //new CosmosDBAccountLocation(){ LocationName  = AzureLocation.EastUS, FailoverPriority = 1 },
                    //new CosmosDBAccountLocation(){ LocationName  = AzureLocation.CentralUS, FailoverPriority = 2 },
                };
                var dbAccountInput = new CosmosDBAccountCreateOrUpdateContent(AzureLocation.WestUS2, locations)
                {
                    Kind = cosmosDBKind,
                    ConsistencyPolicy = new Azure.ResourceManager.CosmosDB.Models.ConsistencyPolicy(DefaultConsistencyLevel.BoundedStaleness)
                    {
                        MaxStalenessPrefix = _maxStalenessPrefix,
                        MaxIntervalInSeconds = _maxIntervalInSeconds
                    },
                    IPRules =
                    {
                        new CosmosDBIPAddressOrRange()
                        {
                            //IPAddressOrRange = Environment.GetEnvironmentVariable("Current_Machine_PublicIP")
                            IPAddressOrRange = "167.220.233.61"
                        }
                    },
                    IsVirtualNetworkFilterEnabled = true,
                    EnableAutomaticFailover = false,
                    ConnectorOffer = ConnectorOffer.Small,
                    DisableKeyBasedMetadataWriteAccess = false,
                    EnableMultipleWriteLocations = true,
                    PublicNetworkAccess = CosmosDBPublicNetworkAccess.Enabled,
                };

                dbAccountInput.Tags.Add("key1", "value");
                dbAccountInput.Tags.Add("key2", "value");
                var accountLro = await resourceGroup.GetCosmosDBAccounts().CreateOrUpdateAsync(WaitUntil.Completed, dbAccountName, dbAccountInput);
                CosmosDBAccountResource dbAccount = accountLro.Value;
                Utilities.Log($"Created CosmosDB {dbAccount.Id.Name}");

                //============================================================
                // Get credentials for the CosmosDB.

                Utilities.Log("Get credentials for the CosmosDB");
                var getKeysLro = await dbAccount.GetKeysAsync();
                CosmosDBAccountKeyList keyList = getKeysLro.Value;
                string masterKey = keyList.PrimaryMasterKey;
                string endPoint = dbAccount.Data.DocumentEndpoint;
                Utilities.Log($"masterKey: {masterKey}");
                Utilities.Log($"endPoint: {endPoint}");

                //============================================================
                // Connect to CosmosDB and add a collection

                Console.WriteLine("Connecting and adding collection");
                CreateDBAndAddCollection(masterKey, endPoint);

                //============================================================
                // Delete CosmosDB
                Utilities.Log("Deleting the CosmosDB");
                try
                {
                    await dbAccount.DeleteAsync(WaitUntil.Completed);
                }
                catch (Exception ex)
                {
                    Utilities.Log(ex.ToString());
                }
                Utilities.Log("Deleted the CosmosDB");
            }
            catch (Exception ex)
            {
                Utilities.Log(ex);
            }
            finally
            {
                try
                {
                    if (_resourceGroupId is not null)
                    {
                        Utilities.Log($"Deleting Resource Group: {_resourceGroupId}");
                        await client.GetResourceGroupResource(_resourceGroupId).DeleteAsync(WaitUntil.Completed);
                        Utilities.Log($"Deleted Resource Group: {_resourceGroupId}");
                    }
                }
                catch (NullReferenceException)
                {
                    Utilities.Log("Did not create any resources in Azure. No clean up is necessary");
                }
                catch (Exception e)
                {
                    Utilities.Log(e.StackTrace);
                }
            }
        }

        private static void CreateDBAndAddCollection(string masterKey, string endPoint)
        {
            DocumentClient documentClient = new DocumentClient(new System.Uri(endPoint),
                    masterKey, ConnectionPolicy.Default,
                    ConsistencyLevel.Session);

            // Define a new database using the id above.
            Database myDatabase = new Database();
            myDatabase.Id = DATABASE_ID;

            myDatabase = documentClient.CreateDatabaseAsync(myDatabase, null)
                    .GetAwaiter().GetResult();

            Console.WriteLine("Created a new database:");
            Console.WriteLine(myDatabase.ToString());

            // Define a new collection using the id above.
            DocumentCollection myCollection = new DocumentCollection();
            myCollection.Id = COLLECTION_ID;

            // Set the provisioned throughput for this collection to be 1000 RUs.
            RequestOptions requestOptions = new RequestOptions();
            requestOptions.OfferThroughput = 4000;

            // Create a new collection.
            myCollection = documentClient.CreateDocumentCollectionAsync(
                    "dbs/" + DATABASE_ID, myCollection, requestOptions)
                    .GetAwaiter().GetResult();
        }

        public static async Task Main(string[] args)
        {
            try
            {
                //=================================================================
                // Authenticate
                var clientId = Environment.GetEnvironmentVariable("CLIENT_ID");
                var clientSecret = Environment.GetEnvironmentVariable("CLIENT_SECRET");
                var tenantId = Environment.GetEnvironmentVariable("TENANT_ID");
                var subscription = Environment.GetEnvironmentVariable("SUBSCRIPTION_ID");
                ClientSecretCredential credential = new ClientSecretCredential(tenantId, clientId, clientSecret);
                ArmClient client = new ArmClient(credential, subscription);

                await RunSample(client);
            }
            catch (Exception e)
            {
                Utilities.Log(e.Message);
                Utilities.Log(e.StackTrace);
            }
        }
    }
}
