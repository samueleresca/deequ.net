# deequ.NET - Yelp academic data test example

The following examples use the following [Yelp academic data set](https://www.kaggle.com/yelp-dataset/yelp-dataset) to test the deequ.NET library.

## Getting started

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

## Azure - Deploy the solution

