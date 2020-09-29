# deequ.NET - Yelp academic data test example

The following examples use the following [Yelp academic data set](https://www.kaggle.com/yelp-dataset/yelp-dataset) to test the deequ.NET library.

# Getting started

The project contains some data sample in the `local_data` folder.
It is possible to specify the path to the data for the local execution in the `appsettings.json`.
In order to run the application locally it is necessary to have the following prerequisites:

- [Mac Os](https://github.com/dotnet/spark/blob/master/docs/getting-started/macos-instructions.md#pre-requisites)
- [Ubuntu](https://github.com/dotnet/spark/blob/master/docs/getting-started/ubuntu-instructions.md#pre-requisites)
- [Windows]((https://github.com/dotnet/spark/blob/master/docs/getting-started/windows-instructions.md#pre-requisites))

Then we can proceed by running the project in the following way:

- Building the project using `dotnet build`;
- Running the project by moving in the `bin/Debug/netcoreapp3.1/` and trigger the following command:
```bash
spark-submit \
    --class org.apache.spark.deploy.dotnet.DotnetRunner \
    --master local \
    microsoft-spark-2.4.x-0.12.1.jar \
dotnet YelpAcademicDataTests.dll
```

# Azure - Deploy the solution

The following section shows how to run the example with the full kaggle dataset on Azure using the following Azure products:

- Azure Storage;
- Azure HDInsights;

## Initialize the storage

https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-cli
Let's start by creating the Azure Storage needed to host the [Yelp dataset](https://www.kaggle.com/yelp-dataset/yelp-dataset).
The following process will create:
- A dedicated resource group
- The storage account
- The container

First of all, let's proceed by creating the resource group using the following command:

```
az group create \
    --name <resource-group> \
    --location <location>
```
After the creation of the resource group we can proceed by creating the storage account using the `<resource-group>` we just created:
```
az storage account create \
    --name <storage-account> \
    --resource-group <resource-group> \
    --location <location> \
    --sku Standard_ZRS
```
Once we created the storage account we can proceed by creating a container for hosting the [Yelp dataset](https://www.kaggle.com/yelp-dataset/yelp-dataset) using the following command:


```
az ad signed-in-user show --query objectId -o tsv | az role assignment create \
    --role "Data Reader" \
    --assignee @- \
    --scope "/subscriptions/<subscription_id>/resourceGroups/<resource_group>/providers/Microsoft.Storage/storageAccounts/<storage_account>"

az storage container create \
    --account-name <storage_account> \
    --name <container_name> \
    --auth-mode login
```

## Deploy the dataset

Once we have the following information: `<storage_account>`, `<containier_name>` we can proceed by copying the files of the [Yelp dataset](https://www.kaggle.com/yelp-dataset/yelp-dataset) into the container
using the [AzCopy CLI](https://docs.microsoft.com/en-us/azure/storage/common/storage-use-azcopy-v10):

```
azcopy login
azcopy copy '<path_to_file>\yelp_academic_dataset_business.json' 'https://<storage_account>.blob.core.windows.net/<containier_name>/yelp_academic_dataset_business.json'
azcopy copy '<path_to_file>\yelp_academic_dataset_review.json' 'https://<storage_account>.blob.core.windows.net/<containier_name>/yelp_academic_dataset_review.json'
```
The above commands will upload the `yelp_academic_dataset_business.json` and the `yelp_academic_dataset_review.json` fields of the Yelp dataset into the container in Azure.
Once the files are uploaded to Azure Storage we can proceed by creating a new `appsettings.<env>.json` file that points to the corresponding files:

```
{
    "dataset_business": "wasbs://<container>@<storage_account>.blob.core.windows.net/yelp_academic_dataset_review.json",
    "dataset_review": "wasbs://<container>@<storage_account>.blob.core.windows.net/yelp_academic_dataset_business.json"
}
```

## Deploy the solution files

## Create the cluster for processing data

