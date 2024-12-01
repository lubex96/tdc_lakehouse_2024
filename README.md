# Lakehouse in action

## Installing required softwares

### Java 17

#### Mac OS

- [Oracle • JDK Installation Guide](https://docs.oracle.com/en/java/javase/17/install/installation-jdk-macos.html)

Alternetively, you can install using [HomeBrew](https://brew.sh):

```shell
brew install openjdk@17
sudo ln -sfn /usr/local/opt/openjdk@17/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-17.jdk
echo 'export PATH="/usr/local/opt/openjdk@17/bin:$PATH"' >> ~/.zshrc
java --version
```

#### Linux (Ubuntu/Debian)

- [Oracle • JDK Installation Guide](https://docs.oracle.com/en/java/javase/17/install/installation-jdk-linux-platforms.html)

Alternetively, you can install using `apt`:

```shell
sudo apt update
sudo apt install openjdk-17-jdk
java --version
```

#### Windows

- [Oracle • JDK Installation Guide](https://docs.oracle.com/en/java/javase/17/install/installation-jdk-microsoft-windows-platforms.html)

### Python 3

#### Mac OS

- [Official Python download page](https://www.python.org/downloads/)

Alternetively, you can install using [HomeBrew](https://brew.sh):

```shell
brew install python3
python3 --version
pip3 --version
```

#### Linux (Ubuntu/Debian)

- [Official Python download page](https://www.python.org/downloads/)

Alternetively, you can install using `apt`:

```shell
sudo apt update
sudo apt install python3
python3 --version
pip3 --version
```

#### Windows

- [Official Python download page](https://www.python.org/downloads/)

### Python packages

#### Create a virtual environment

Let's start by creating a virtual environment to isolate your project's dependencies from your global Python installation:

```shell
python3 -m venv lakehouse-env
```

To activate the environment:

```shell
source lakehouse-env/bin/activate
```

#### Install Jupyter

Now, let's install Jupyter Lab/Notebook:

```shell
pip3 install jupyter
```

Verify the installation by running the following command:

```shell
jupyter lab
```

#### Install PySpark 3.5.3

To install PySpark, run:

```shell
pip3 install pyspark==3.5.3
```

To verify the installation:

```shell
pyspark --version
```

#### Install other Python libraries

Finally, we are going to install the following libraries:

- Delta Sharing
- Pandas
- MatPlotLib
- PyJWT
- Cryptography

To install, run:

```shell
pip3 install delta-sharing pandas matplotlib pyjwt cryptography
```

## Running Spark

After installing all the necessary dependencies, you are now ready to run Spark.

Let's start Jupyter Lab where you will be able to create and run Python Notebooks.

```shell
jupyter lab
```

Create a new notebook by clicking on File > New > Notebook.

### Running a sample Spark app

To use PySpark on your notebook, first of all you need to create a Spark Session.

The following snippet creates a Spark session, a very simple DataFrame and displays the DataFrame content.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MySampleApp") \
    .getOrCreate()

data = [("Alice", 29), ("Bob", 31), ("Cathy", 22)]
columns = ["Name", "Age"]

df = spark.createDataFrame(data, columns)
df.show()
```

> Note: the first time you run this code, it will probably take some time for Spark to initialize.

### Integrating with Delta Lake

Now, let's integrate your Spark application with the Delta Lake libraries.

- [Delta Lake • Quick Start](https://docs.delta.io/latest/quick-start.html#set-up-apache-spark-with-delta-lake)

To leverage Delta Lake, you need to configure your Spark session to:

1. Load the Delta Lake Java/Scala libraries during Spark initialization
2. Use the Delta Spark catalog and Session extension

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Getting started with Spark and Delta Lake") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
```

After the Spark session is created, you can generate a dummy Spark DataFrame and write it as a Delta table:

```python
deltaTablePath = "resources/tmp/sample-delta-table"

data = spark.range(0, 5)
data.write.format("delta").save(deltaTablePath)
```

Finally, you can read the Delta table created above to a Spark DataFrame and print its content.

```python
df = spark.read.format("delta").load(deltaTablePath)
df.show()
```

### Integrating with SAP HANA Cloud, Data Lake Files

In the above example, the Delta table was written to your local file system (your disk).
In this step, we are going to configure Spark to use [SAP HANA Cloud, Data Lake Files](https://help.sap.com/docs/hana-cloud-data-lake) (aka. HDL Files/HDLF) as the storage for your data.

First of all, you need to have an HDLF FileContainer where your data will be stored.
The authentication between your Spark application and your FileContainer is based on x509 certificates that are configured as trusted/authorized to access the data.

An HDLF FileContainer was previously prepared for this workshop session.
The URL to access it is the following:

**TODO** Adjust the CFC URL.

```
hdlfs://cfcselfsigned1.files.hdl.demo-hc-3-hdl-hc-dev.dev-aws.hanacloud.ondemand.com
```

You can obtain the certificates to authenticate against this FileContainer accessing the following link:

**TODO** Link to download the certs.

The Spark session should be configured as follows. Replace the `<path-to-certs>` placeholder with the path where you downloaded the certificates.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Getting started with Spark, Delta Lake and HDL Files") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0,com.sap.hana.datalake.files:sap-hdlfs:3.0.27") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.AbstractFileSystem.hdlfs.impl", "com.sap.hana.datalake.files.Hdlfs") \
    .config("spark.hadoop.fs.hdlfs.impl", "com.sap.hana.datalake.files.HdlfsFileSystem") \
    .config("spark.hadoop.fs.defaultFS", "<hdlfs-endpoint-url>") \
    .config("spark.hadoop.fs.hdlfs.ssl.certfile", "<path-to-certs>/client.crt") \
    .config("spark.hadoop.fs.hdlfs.ssl.keyfile", "<path-to-certs>/client.key") \
    .getOrCreate()
```

To validate the configuration, you can write a Delta table using the same commands that were used in the [Integrating with Delta Lake](#integrating-with-delta-lake) section, but now, the data will be stored on the object store.

> NOTE: Considering that the same FileContainer is being shared between all the workshop participants, **please isolate your data by providing a unique prefix when specifying the path where your Delta table will be written**.

```python
deltaTablePath = "/<my-full-name>/sample-delta-table"

data = spark.range(0, 5)
data.write.format("delta").save(deltaTablePath)

df = spark.read.format("delta").load(deltaTablePath)
df.show()
```

Execution may take longer this time since the data is being stored remotely.
Latency tends to be high since FileContainer is deployed in a data center in Europe.

### Transforming data using Spark

**TODO** Instructions to process the COVID dataset and store the result in Delta table(s).

## Sharing data using Delta Sharing

After processing the data, you may want to share the outcome with external users/systems.
For that, you can leverage [Delta Sharing](https://delta.io/sharing/), which is an open source protocol that enables data to be securely shared with authorized recipients.

To share Delta tables stored in HDL Files, you need to:

1. Register a Share in the HDLF Catalog.
2. Create a Share Table in the HDLF Catalog that points to the Delta table location in your FileContainer.
3. Generate a JWT that gives access to the Share.
4. Provide your authorized recipeint with this JWT (through a secure channel).

HDL Files offers a Catalog API that can be used to manage your shares and share tables.
For simplicity, you can use the script [share-delta-table.py](./scripts/share-delta-table.py) to cover steps 1 and 2.

> NOTE: Choose a unique name for your share, to avoid conflict with other participants of the workshop.

```shell
python3 scripts/share-delta-table.py \
    --hdlfEndpoint <hdlf-endpoint> \
    --clientCertPath <path-to-certs>/client.crt \
    --clientKeyPath <path-to-certs>/client.key \
    --shareName <my-unique-share-name> \
    --shareSchema <schema-name> \
    --shareTableName <table-name> \
    --deltaTableLocation <path-to-delta-table>
```

After the Share and Share table(s) are created, you can generate a JWT with enough permissions to access the data in the share.
The script [generate-delta-sharing-profile.py](./scripts/generate-delta-sharing-profile.py) generates the HDLF JWT, authorizing the only the share(s) you want, and generates a Delta sharing profile file that can be shared with the recipient you want to provide access to.

```shell
python3 scripts/generate-delta-sharing-profile.py \
    --hdlfEndpoint <hdlf-endpoint> \
    --clientCertPath <path-to-certs>/client.crt \
    --clientKeyPath <path-to-certs>/client.key \
    --authorizedShares <my-share-name> \
    --outputFilePath <delta-sharing-profile-output-path>
```
