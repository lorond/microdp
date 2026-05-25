CREATE EXTERNAL CATALOG IF NOT EXISTS demo_lake
COMMENT "MicroDP Iceberg catalog on Garage (S3)"
PROPERTIES
(
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "iceberg.catalog.uri" = "http://nessie:19120/iceberg",
  "iceberg.catalog.warehouse" = "warehouse",
  "aws.s3.access_key" = "${S3_ACCESS_KEY}",
  "aws.s3.secret_key" = "${S3_SECRET_KEY}",
  "aws.s3.endpoint" = "http://s3:3900",
  "aws.s3.enable_path_style_access" = "true",
  "aws.s3.region" = "us-east-1"
);

