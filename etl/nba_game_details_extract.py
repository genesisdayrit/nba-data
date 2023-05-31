import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="nba-data",
    table_name="games_details_csv",
    transformation_ctx="S3bucket_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("game_id", "long", "game_id", "long"),
        ("team_id", "long", "team_id", "long"),
        ("team_abbreviation", "string", "team_abbreviation", "string"),
        ("team_city", "string", "team_city", "string"),
        ("player_id", "long", "player_id", "long"),
        ("player_name", "string", "player_name", "string"),
        ("nickname", "string", "nickname", "string"),
        ("start_position", "string", "start_position", "string"),
        ("comment", "string", "comment", "string"),
        ("min", "string", "min", "string"),
        ("fgm", "double", "fgm", "double"),
        ("fga", "double", "fga", "double"),
        ("fg_pct", "double", "fg_pct", "double"),
        ("fg3m", "double", "fg3m", "double"),
        ("fg3a", "double", "fg3a", "double"),
        ("fg3_pct", "double", "fg3_pct", "double"),
        ("ftm", "double", "ftm", "double"),
        ("fta", "double", "fta", "double"),
        ("ft_pct", "double", "ft_pct", "double"),
        ("oreb", "double", "oreb", "double"),
        ("dreb", "double", "dreb", "double"),
        ("reb", "double", "reb", "double"),
        ("ast", "double", "ast", "double"),
        ("stl", "double", "stl", "double"),
        ("blk", "double", "blk", "double"),
        ("to", "double", "turnovers", "double"),
        ("pf", "double", "pf", "double"),
        ("pts", "double", "pts", "double"),
        ("plus_minus", "double", "plus_minus", "double"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Amazon Redshift
AmazonRedshift_node1685405658671 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMapping_node2,
    connection_type="redshift",
    connection_options={
        "postactions": "BEGIN; DELETE FROM public.game_details USING public.game_details_temp_a65a51 WHERE public.game_details_temp_a65a51.game_id = public.game_details.game_id; INSERT INTO public.game_details SELECT * FROM public.game_details_temp_a65a51; DROP TABLE public.game_details_temp_a65a51; END;",
        "redshiftTmpDir": "s3://aws-glue-assets-758443678568-us-east-2/temporary/",
        "useConnectionProperties": "true",
        "dbtable": "public.game_details_temp_a65a51",
        "connectionName": "redshift-connection",
        "preactions": "CREATE TABLE IF NOT EXISTS public.game_details (game_id BIGINT, team_id BIGINT, team_abbreviation VARCHAR, team_city VARCHAR, player_id BIGINT, player_name VARCHAR, nickname VARCHAR, start_position VARCHAR, comment VARCHAR, min VARCHAR, fgm DOUBLE PRECISION, fga DOUBLE PRECISION, fg_pct DOUBLE PRECISION, fg3m DOUBLE PRECISION, fg3a DOUBLE PRECISION, fg3_pct DOUBLE PRECISION, ftm DOUBLE PRECISION, fta DOUBLE PRECISION, ft_pct DOUBLE PRECISION, oreb DOUBLE PRECISION, dreb DOUBLE PRECISION, reb DOUBLE PRECISION, ast DOUBLE PRECISION, stl DOUBLE PRECISION, blk DOUBLE PRECISION, turnovers DOUBLE PRECISION, pf DOUBLE PRECISION, pts DOUBLE PRECISION, plus_minus DOUBLE PRECISION); DROP TABLE IF EXISTS public.game_details_temp_a65a51; CREATE TABLE public.game_details_temp_a65a51 AS SELECT * FROM public.game_details WHERE 1=2;",
    },
    transformation_ctx="AmazonRedshift_node1685405658671",
)

job.commit()
