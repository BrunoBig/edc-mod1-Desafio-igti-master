
resource "aws_s3_bucket_object" "delta_insert" {
  bucket = aws_s3_bucket.dl.id
  key    = "emr-code/pyspark/01_delta_spark_insert.py"
  acl    = "private"
  source = "../etl/AbreCSV_SalvaPARQUET.py"
  etag   = filemd5("../etl/AbreCSV_SalvaPARQUET.py")
}

