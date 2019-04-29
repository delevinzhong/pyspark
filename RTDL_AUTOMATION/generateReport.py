import sys
from pyspark.shell import spark
from pyspark.sql.functions import *
import pyspark.sql.functions as F
import pandas as pd


def generate_daily_report(error_count, missing_count, total_rcv_count, cur_date, database_name):
    # A query to get the messages for cur_date
    sql = "SELECT SUBSTR(X791266_GFMT_CTM_UID,1,4) AS CTM_UID,COUNT(*) AS TOTAL_COUNT FROM %s WHERE SUBSTR(" \
          "PARTITION_COL,1,8) = %s GROUP BY SUBSTR(X791266_GFMT_CTM_UID,1,4)" % (database_name, cur_date)

    # Create a data frame regarding the total received messages for cur_date
    total_df = spark.sql(sql)

    missing_csv = ""
    try:
        miss_msg_df = spark.read.option("header", "true").csv(missing_csv)

    except FileNotFoundError:
        print("Missing uniq id file does not exist")
        exit(1)

    add_non_null_df = total_df.withColumn("tag_for_non_null", when(col("CTM_UID").__eq__(""), lit("1")).
                                          otherwise(lit("0")))
    add_non_ctm_df = add_non_null_df.withColumn("tag_for_non_ctm", when(col("CTM_UID").startswith("0"), lit("1")).
                                                otherwise(lit("0")))

    filter_df = add_non_ctm_df.select(col("CTM_UID"), col("TOTAL_COUNT")).filter(col("tag_for_non_null").__eq__("0").
                                                                                 __and__(col("tag_for_non_ctm").
                                                                                         __eq__("0")))

    filter_df = filter_df.alias("filter_df")
    miss_msg_df = miss_msg_df.alias("miss_msg_df")

    join_df = filter_df.join(miss_msg_df, col("filter_df.CTM_UID") == col("miss_msg_df.miss_ctm_uid"), "full").drop(
        col("miss_msg_df.miss_ctm_uid"))

    stage1_df = join_df.withColumn("miss_count", when(col("miss_count").isNull(), lit("0")).
                                   otherwise(lit(col("miss_count"))))

    stage2_df = stage1_df.withColumn("Missing/UID_total", (F.col("miss_count") /
                                                           (F.col("Total_Count") + F.col("miss_count"))))

    total_count = error_count + missing_count + total_rcv_count

    stage3_df = stage2_df.withColumn("missing/total", (F.col("miss_count") / total_count))

    sort_df = stage3_df.orderBy("CTM_UID", ascending=True)

    pandas_df = sort_df.toPandas()
    pandas_df.to_excel("/xxx/xxx/xxx/%s.xlsx" % cur_date, index=False)


def generate_sumarize_report(date, total_rcv_count, missing_count, error_count, database_name):
    sql = "SELECT SUBSTR(X791266_GFMT_CTM_UID,1,4) AS CTM_UID,COUNT(*) AS TOTAL_COUNT FROM %s WHERE SUBSTR(" \
          "PARTITION_COL,1,8) = %s GROUP BY SUBSTR(X791266_GFMT_CTM_UID,1,4)" % (database_name, date)

    total_df = spark.sql(sql)

    try:
        null_count = total_df.select(col("Total_Count")).filter(col("CTM_UID").__eq__("")).collect()[0]["TOTAL_COUNT"]
    except IndexError:
        print("There's no null value")
        null_count = 0
    try:
        non_ctm_id = total_df.select(col("TOTAL_COUNT")).filter(col("CTM_UID").__eq__("")).collect()[0]['CTM_UID']
        non_ctm_count = total_df.select(col("TOTAL_COUNT")).filter(col("CTM_UID").startswith("0")).collect()[0][
            'Total_COUNT']
    except IndexError:
        print("There's no non CTM ID")
        non_ctm_id = ""
        non_ctm_count = 0

    xlsx_file = ""
    try:
        report_file = pd.ExcelFile(xlsx_file)
        df = pd.read_excel(report_file)
    except FileNotFoundError:
        print("File %s does not exist" % xlsx_file)
        exit(1)

    total_count = int(missing_count) + int(error_count) + int(total_rcv_count)

    cur_day_sumarize_df = pd.DataFrame({"DATE": [str(date)],
                                        "NULL_COUNT": [str(null_count)],
                                        "NON_CTM_ID": [str(non_ctm_id)],
                                        "NON_CTM_COUNT": [str(non_ctm_count)],
                                        "TOTAL_RECEIVED_COUNT": [str(total_rcv_count)],
                                        "MISSING_COUNT": [str(missing_count)],
                                        "ERROR_COUNT": [str(error_count)],
                                        "TOTAL_COUNT": [str(total_count)]})
    columns = ['DATE', 'NULL_COUNT', 'NON_CTM_ID', 'NON_CTM_COUNT', 'TOTAL_RECEIVED_COUNT', 'MISSING_COUNT',
               'ERROR_COUNT', 'TOTAL_COUNT']

    df.append(cur_day_sumarize_df.to_excel('/xxx/xxx/xxx.xlsx', index=False, columns=columns))


if __name__ == '__main__':
    error_count = sys.argv[1]
    missing_count = sys.argv[2]
    total_rcv_count = sys.argv[3]
    cur_date = sys.argv[4]
    database_name = sys.argv[5]

    generate_daily_report(error_count, missing_count, total_rcv_count, cur_date, database_name)

    generate_sumarize_report(cur_date, total_rcv_count, missing_count, error_count, database_name)
