
# example Release 0.12.1 netcoreapp3.1 $SPARK_RUNNER_244


set +e

PROJECT_CONFIG=$1
SPARK_RUNNER=$2
DOTNET_ALIAS=$3
SPARK_PATH=$4

echo "PROJECT_CONFIG ${PROJECT_CONFIG}"
echo "SPARK_RUNNER ${SPARK_RUNNER}"
echo "DOTNET_ALIAS ${DOTNET_ALIAS}"
echo "SPARK_PATH ${SPARK_PATH}"

cd bin/$PROJECT_CONFIG/$DOTNET_ALIAS/

 $SPARK_PATH/spark-submit \
    --class org.apache.spark.deploy.dotnet.DotnetRunner \
    --master local \
    $SPARK_RUNNER \
    dotnet example.dll
