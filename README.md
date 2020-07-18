# xdeequ



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
