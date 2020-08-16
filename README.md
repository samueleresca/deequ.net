# deequ.NET

[![deequ.NET](https://github.com/samueleresca/deequ.net/workflows/deequ.NET/badge.svg)](https://github.com/samuelereca/deeuqu.NET)
[![codecov](https://codecov.io/gh/samueleresca/deequ.net/branch/master/graph/badge.svg)](https://codecov.io/gh/samueleresca/deequ.net)
[![Nuget](https://img.shields.io/nuget/vpre/deequ)](https://www.nuget.org/packages/deequ)
[![NuGet](https://img.shields.io/nuget/dt/deequ)](https://www.nuget.org/packages/deequ)

**deequ.NET** is a port of the [awslabs/deequ](https://github.com/awslabs/deequ) library built on top of Apache Spark for defining "unit tests for data", which measure data quality in large datasets.
deequ.NET runs on [dotnet/spark](https://github.com/dotnet/spark).

## Requirements and Installation



```csharp
            double sizeThreshold;
            var verificationResultOne = new VerificationSuite()
                .OnData(df)
                .AddCheck(new Check(CheckLevel.Error, "Testing my data")
                    .HasSize(x=>x > sizeThreshold, Option<string>.None)
                    .IsComplete("restaurantId", "should not contain NULLS")
                    .IsUnique("restaurantId", "should not contain duplicates")
                    .IsComplete("title", "should not contain NULLS")
                    .IsNonNegative("restaurantCount", "should not contain negative values")
                    .HasApproxQuantile("restaurantId", 0.9, (_) => _ == 20,
                        "90% of the items should have at least 20 restaurantIds")
                ).Run();


            (verificationResultOne.Status == CheckStatus.Success).ShouldBeTrue();

```
