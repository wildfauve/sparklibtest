import pyspark
from pyspark.sql.functions import lit
import pendulum

def with_lineage(df):
    return df.withColumn("_loadtime", lit(pendulum.now("Europe/Paris")))


def job():
    return True