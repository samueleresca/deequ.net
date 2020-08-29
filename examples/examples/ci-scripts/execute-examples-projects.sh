
# example Release 0.12.1 netcoreapp3.1 $SPARK_RUNNER_244


set +e

PROJECT_CONFIG=$1
DOTNET_SPARK_VERSION=$2
DOTNET_ALIAS=$3
SPARK_PATH=$4

echo "PROJECT_CONFIG ${PROJECT_CONFIG}"
echo "DOTNET_SPARK_VERSION ${DOTNET_SPARK_VERSION}"
echo "DOTNET_ALIAS ${DOTNET_ALIAS}"
echo "SPARK_PATH ${SPARK_PATH}"

cd bin/$PROJECT_CONFIG/$DOTNET_ALIAS/

 $SPARK_PATH/spark-submit \
    --class org.apache.spark.deploy.dotnet.DotnetRunner \
    --master local \
    microsoft-spark-2.4.x-$DOTNET_SPARK_VERSION.jar \
    dotnet example.dll
