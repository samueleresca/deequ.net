
# example Release 0.12.1 netcoreapp3.1 $SPARK_RUNNER_244


set +e

PROJECT_NAME=$1
PROJECT_CONFIG=$2
DOTNET_SPARK_VERSION=$3
DOTNET_ALIAS=$4
$SPARK_PATH=$5

echo "PROJECT_NAME ${PROJECT_NAME}"
echo "PROJECT_CONFIG ${PROJECT_CONFIG}"
echo "DOTNET_SPARK_VERSION ${DOTNET_SPARK_VERSION}"
echo "DOTNET_ALIAS ${DOTNET_ALIAS}"
echo "$SPARK_PATH ${SPARK_PATH}"

 $SPARK_PATH/spark-submit \
    --class org.apache.spark.deploy.dotnet.DotnetRunner \
    --master local \
    microsoft-spark-2.4.x-$DOTNET_SPARK_VERSION.jar \
    dotnet "/bin/$PROJECT_CONFIG/$DOTNET_ALIAS/$PROJECT_NAME.dll"
