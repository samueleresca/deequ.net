# deequ.NET

[![deequ.NET](https://github.com/samueleresca/deequ.net/workflows/deequ.NET/badge.svg)](https://github.com/samuelereca/deeuqu.NET)
[![codecov](https://codecov.io/gh/samueleresca/deequ.net/branch/master/graph/badge.svg)](https://codecov.io/gh/samueleresca/deequ.net)
[![Nuget](https://img.shields.io/nuget/vpre/deequ)](https://www.nuget.org/packages/deequ)
[![NuGet](https://img.shields.io/nuget/dt/deequ)](https://www.nuget.org/packages/deequ)

**deequ.NET** is a port of the [awslabs/deequ](https://github.com/awslabs/deequ) library built on top of Apache Spark for defining "unit tests for data", which measure data quality in large datasets.
deequ.NET runs on [dotnet/spark](https://github.com/dotnet/spark).

## Requirements and Installation

deequ.NET runs on Apache Spark and depends on [dotnet/spark](https://github.com/dotnet/spark). Therefore it is required to install the following dependencies locally:


 - [.NET Core 3.1 SDK](https://dotnet.microsoft.com/download/dotnet-core/3.1)
 - [OpenJDK 8](https://openjdk.java.net/install/)
 - [Apache Spark 2.3 +](https://archive.apache.org/dist/spark/)

It is also necessary to install the [Microsoft.Spark.Worker](https://github.com/dotnet/spark/releases) on your local machine and configure the path into the `PATH` env var.
For a detailed instructions,  see [dotnet/spark - Getting started](https://github.com/dotnet/spark/tree/master/docs/getting-started)

## Example

The following example implements a set of checks on some records and it submits the execution using the `spark-submit` command.


- Use the `dotnet` CLI to create a console application:

   ```shell
   dotnet new console -o DeequExample
   ```
- Install `Microsoft.Spark` and the `deequ` Nuget packages into the project:

    ```shell
    cd DeequExample

    dotnet add package Microsoft.Spark
    dotnet add package deequ
    ```
- Replace the contents of the `Program.cs` file with the following code:

    ```csharp
    using deequ;
    using deequ.Checks;
    using deequ.Extensions;
    using Microsoft.Spark.Sql;

    namespace DeequExample
    {
        class Program
        {
            static void Main(string[] args)
            {
                SparkSession spark = SparkSession.Builder().GetOrCreate();
                DataFrame data = spark.Read().Json("inventory.json");

                data.Show();

                VerificationResult verificationResult = new VerificationSuite()
                    .OnData(data)
                    .AddCheck(
                        new Check(CheckLevel.Error, "integrity checks")
                            .HasSize(value => value == 5)
                            .IsComplete("id")
                            .IsUnique("id")
                            .IsComplete("productName")
                            .IsContainedIn("priority", new[] { "high", "low" })
                            .IsNonNegative("numViews")
                    )
                    .AddCheck(
                        new Check(CheckLevel.Warning, "distribution checks")
                            .ContainsURL("description", value => value >= .5)
                    )
                    .Run();

                verificationResult.Debug();
            }
        }
    }
    ```
- Use the `dotnet` CLI to build the application:

    ```shell
    dotnet build
    ```

### Running the example

- Open your terminal and navigate into your app folder.

    ```shell
    cd <your-app-output-directory>
    ```
- Create `inventory.json` with the following content:

    ```json
    {"id":1, "productName":"Thingy A", "description":"awesome thing. http://thingb.com", "priority":"high", "numViews":0}
    {"id":2, "productName":"Thingy B", "description":"available at http://thingb.com","priority":null, "numViews":0}
    {"id":3, "productName":"Thingy C", "description": null, "priority":"low", "numViews":5}
    {"id":4, "productName":"Thingy D", "description": "checkout https://thingd.ca", "priority":"low","numViews": 10}
    {"id":5, "productName":"Thingy E", "description":null, "priority":"high","numViews": 12}
    ```
- Run your app.

    ```shell
    spark-submit \
        --class org.apache.spark.deploy.dotnet.DotnetRunner \
        --master local \
        microsoft-spark-2.4.x-<version>.jar \
    dotnet DeequExample.dll
    ```
    **Note**: This command requires Apache Spark in your PATH environment variable to be able to use `spark-submit`. For detailed instructions, you can see [Building .NET for Apache Spark from Source on Ubuntu](../building/ubuntu-instructions.md).
- The output of the application should look similar to the output below:

    ```text

         _                         _   _ ______ _______
        | |                       | \ | |  ____|__   __|
      __| | ___  ___  __ _ _   _  |  \| | |__     | |
     / _` |/ _ \/ _ \/ _` | | | | | . ` |  __|    | |
    | (_| |  __/  __/ (_| | |_| |_| |\  | |____   | |
     \__,_|\___|\___|\__, |\__,_(_)_| \_|______|  |_|
                        | |
                        |_|



    Success
    ```

## Citation

If you would like to reference this package in a research paper, please cite:

Sebastian Schelter, Dustin Lange, Philipp Schmidt, Meltem Celikel, Felix Biessmann, and Andreas Grafberger. 2018. [Automating large-scale data quality verification](http://www.vldb.org/pvldb/vol11/p1781-schelter.pdf). Proc. VLDB Endow. 11, 12 (August 2018), 1781-1794.
##
