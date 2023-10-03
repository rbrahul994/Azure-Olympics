from pyspark.sql.functions import col
from pyspark.sql.types import IntergerType, DoubleType, BooleanType, DateType

configs = {"fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
    "fs.azure.account.oauth2.client.id": "<application-id>"
    "fs.azure.account.oauth2.client.secret": "{{secrets/<secret-scope>/<service-credential-key>}}"
    "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<directory-id>/oauth2/token"
}

dbutils.fs.mount(
    source = "<container-name>@<storage_account_name>.dfs.core.windows.net",
    mount_point = "/mnt/tokyoolympic",
    extra_configs = configs
)

%fs
ls "mnt/tokyoolympic"


athletes = spark.read.format("csv").option("header", "true").load("/mnt/tokyoolympic/raw-data/athletes.csv")
coaches = spark.read.format("csv").option("header", "true").load("/mnt/tokyoolympic/raw-data/coaches.csv")
entriesgender = spark.read.format("csv").option("header", "true").load("/mnt/tokyoolympic/raw-data/entriesgender.csv")
medals = spark.read.format("csv").option("header", "true").load("/mnt/tokyoolympic/raw-data/medals.csv")
teams = spark.read.format("csv").option("header", "true").load("/mnt/tokyoolympic/raw-data/teams.csv")


athletes.show()

athletes.printschema()

# manually update  column

entriesgender = entriesgender.withColumn("Female", col("Female").cast(IntergerType()))\
    .withColumn("Male", col("Male").cate(IntergerType()))\
    .withColumn("Total", col("Total").cate(IntergerType()))

# use inferSchema option to make spark read the data and detect the dataType
athletes = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("/mnt/tokyoolympic/raw-data/athletes.csv")
coaches = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("/mnt/tokyoolympic/raw-data/coaches.csv")
entriesgender = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("/mnt/tokyoolympic/raw-data/entriesgender.csv")
medals = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("/mnt/tokyoolympic/raw-data/medals.csv")
teams = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("/mnt/tokyoolympic/raw-data/teams.csv")


#after the transformation write in the transformed data directory

athletes.repartition(1).write..mode("overwrite")option("header", "true").csv("/mnt/tokyoolympic/transformed-data/athletes")
coaches.repartition(1).write..mode("overwrite")option("header", "true").csv("/mnt/tokyoolympic/transformed-data/coaches")
entriesgender.repartition(1).write..mode("overwrite")option("header", "true").csv("/mnt/tokyoolympic/transformed-data/entriesgender")
medals.repartition(1).write..mode("overwrite")option("header", "true").csv("/mnt/tokyoolympic/transformed-data/medals")
teams.repartition(1).write..mode("overwrite")option("header", "true").csv("/mnt/tokyoolympic/transformed-data/teams")