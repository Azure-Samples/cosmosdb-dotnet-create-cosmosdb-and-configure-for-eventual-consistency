// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using Azure;
using Azure.Core;
using Azure.Identity;
using Azure.ResourceManager.Resources.Models;
using Azure.ResourceManager.Samples.Common;
using Azure.ResourceManager.Resources;
using Azure.ResourceManager;

namespace CosmosDBWithEventualConsistency
{

    public class Program
    {
        private static ResourceIdentifier? _resourceGroupId = null; 
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
            string cosmosDBName = SdkContext.RandomResourceName("docDb", 10);
            string rgName = SdkContext.RandomResourceName("rgNEMV", 24);

            try
            {
                // Get default subscription
                SubscriptionResource subscription = await client.GetDefaultSubscriptionAsync();

                // Create a resource group in the EastUS region
                string rgName = Utilities.CreateRandomName("CosmosDBTemplateRG");
                Utilities.Log($"creating resource group with name:{rgName}");
                ArmOperation<ResourceGroupResource> rgLro = await subscription.GetResourceGroups().CreateOrUpdateAsync(WaitUntil.Completed, rgName, new ResourceGroupData(AzureLocation.EastUS));
                ResourceGroupResource resourceGroup = rgLro.Value;
                _resourceGroupId = resourceGroup.Id;
                Utilities.Log("Created a resource group with name: " + resourceGroup.Data.Name);

                //============================================================
                // Create a CosmosDB.

                Console.WriteLine("Creating a CosmosDB...");
                ICosmosDBAccount cosmosDBAccount = azure.CosmosDBAccounts.Define(cosmosDBName)
                        .WithRegion(Region.USWest)
                        .WithNewResourceGroup(rgName)
                        .WithKind(DatabaseAccountKind.GlobalDocumentDB)
                        .WithEventualConsistency()
                        .WithWriteReplication(Region.USEast)
                        .WithReadReplication(Region.USCentral)
                        .Create();

                Console.WriteLine("Created CosmosDB");
                Utilities.Print(cosmosDBAccount);

                //============================================================
                // Get credentials for the CosmosDB.

                Console.WriteLine("Get credentials for the CosmosDB");
                var databaseAccountListKeysResult = cosmosDBAccount.ListKeys();
                string masterKey = databaseAccountListKeysResult.PrimaryMasterKey;
                string endPoint = cosmosDBAccount.DocumentEndpoint;

                //============================================================
                // Connect to CosmosDB and add a collection

                Console.WriteLine("Connecting and adding collection");
                //CreateDBAndAddCollection(masterKey, endPoint);

                //============================================================
                // Delete CosmosDB
                Console.WriteLine("Deleting the CosmosDB");
                // work around CosmosDB service issue returning 404 CloudException on delete operation
                try
                {
                    azure.CosmosDBAccounts.DeleteById(cosmosDBAccount.Id);
                }
                catch (CloudException)
                {
                }
                Console.WriteLine("Deleted the CosmosDB");
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

        private void CreateDBAndAddCollection(string masterKey, string endPoint)
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
