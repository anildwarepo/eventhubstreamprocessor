# EventHub Stream Processor using Spark Structured Streaming

The Event Hub Stream processor can ingest from Azure Event Hub using spark streaming and writes to Azure Data Lake using the specified format. 
The Event Hub Stream Processor is written in Scala. 
It uses Kafka compatible endpoint of Azure Event Hub and hence the same code can be used to ingest from both Azure HDInsight Kafka and Azure Event Hub by modifying boot strap server and connection string.  

The Event Processor is complied as jar and can be submitted using spark-submit to any spark cluster. The Event Hub processor can also be invoked interactively using Scala Notebook.


The event hub processor is compiled as an uber jar with all required dependencies. Please refer to pom.xml for details.  

### Compiling

    mvn clean package

### Connection String for Azure Event Hub

    val BOOTSTRAP_SERVERS = matchEventHubString.getOrElse("").split("=")(1).split("sb://")(1) + ":9093"
        
    //Configure SASL connection string with EventHub connection info
    val EH_SASL = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\"  password=\"" + eventHubConnectionString +  ";EntityPath=" + eventHubTopic + "\";"

    val kafkadf = spark.readStream.format("kafka").option("subscribe", eventHubTopic).option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS).option("kafka.sasl.mechanism", "PLAIN").option("kafka.security.protocol", "SASL_SSL").option("kafka.sasl.jaas.config", EH_SASL).option("kafka.request.timeout.ms", "60000").option("kafka.session.timeout.ms", "60000").option("failOnDataLoss", "false").load()




### Connection String for Azure HDInsight Kafka
Note: Both Azure Databricks and Azure HDInsight Kafka should be within the same Virtual Network. Azure Databricks needs dedicated subnets. 


    val kafkaBrokers="10.1.0.7:9092,10.1.0.8:9092,10.1.0.9:9092" //IP address of Kafka Nodes 
    val kafkaTopic="taxidatatopic"

    val kafkaStreamDF = spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafkaBrokers).option("subscribe", kafkaTopic).option("startingOffsets", "latest").option("groupId","ss-1").load()


### Running on Azure Databricks
Azure Data Lake needs to be configured with Storage Account Name and Storage Account Key in spark config. The Storage Account Key can also be placed in Azure Key Vault and reference using scopes in databricks. 

##### Databricks Job Set Parameter
    
    [--class com.anildwa.EventHubStreamProcessor --conf spark.hadoop.fs.azure.account.key.<azuredatalake account>.dfs.corewindows.net=<Azure Storage Account Access Key or SAS> abfss://jars@azure storage.dfs.core.windows.net/jarsEventHubProcessor.jar abfss://streamdata@anildwaadlsv2.dfs.core.windows.net/input delta Endpoint=sb://<eventhubname>servicebus.windows.net/;SharedAccessKeyName=<SharedAccessKeyName>;SharedAccessKey=<shared access key secret>eventhubtopic]

##### Databricks Notebook Execution

     
    import com.anildwa._
    val ehListener = new EventHubListener()


    ehListener.Start("abfss://container@azuredatalake.dfs.core.windows.net/","delta","Endpoint=sb://eventhubname.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=<EventHub Access Key>", "EventHubTopic")


### Running on Azure Synapse Analytics
Event Hub Stream Processor can also be run on Azure Synapse on Spark pools using Spark Job Definition or using Notebook.
For Synapse though, the Azure Storage Access key need not be passed as the Azure Data Lake would already be added as a Dataset in the Synapse Workspace. 


    [--class com.anildwa.EventHubStreamProcessor abfss://jars@azure storage.dfs.core.windows.net/EventHubProcessor.jar abfss://streamdata@anildwaadlsv2.dfs.core.windows.net/input delta Endpoint=sb://<eventhubname>.servicebus.windows.net/;SharedAccessKeyName=<SharedAccessKeyName>;SharedAccessKey=<shared access key secret> eventhubtopic]


### Running on Kubernetes
Event Hub Stream Processor can also be run on Kubernetes cluster such as Azure Kubernetes Service (AKS).


Start Kubectl proxy 

    $kubectl proxy


Spark Submit

    $./bin/spark-submit     --class com.anildwa.EventHubStreamProcessor    --master k8s://http://127.0.0.1:8001/   --deploy-mode cluster    --num-executors 2    --driver-memory 4g     --executor-memory 4g     --executor-cores 4      --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=abfss://spark-event-logs@azuredatalakestorage.dfs.core.windows.net/logs --conf spark.local.dir=/tmp   --conf spark.kubernetes.driver.volumes.hostPath.aksvm.mount.path=/mnt --conf spark.kubernetes.driver.volumes.hostPath.aksvm.options.path=/tmp  --conf spark.kubernetes.driver.pod.name=spark-driver-pod  --conf spark.kubernetes.local.dirs.tmpfs=true --conf spark.kubernetes.driver.podTemplateFile=/mnt/c/Users/anildwa/source/repos/spark-kube/spark-image/pod-template/driver-pod-template.yaml  --conf spark.kubernetes.executor.podTemplateFile=/mnt/c/Users/anildwa/source/repos/spark-kube/spark-image/pod-template/driver-pod-template.yaml   --conf spark.kubernetes.container.image=anildwa/spark-submit:1.0  --conf spark.kubernetes.namespace=spark  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-sa  --conf spark.hadoop.fs.azure.account.key.azuredatalakestorage.dfs.core.windows.net=<Azure Storage Access Key or SAS>   abfss://jars@azuredatalakestorage.dfs.core.windows.net/EventHubStreamProcessor.jar abfss://stream@azuredatalakestorage.dfs.core.windows.net/input delta Endpoint=sb://<eventhubname>.servicebus.windows.net/;SharedAccessKeyName=<SharedAccessKeyName>;SharedAccessKey=<shared access key secret> eventhubtopic









    
