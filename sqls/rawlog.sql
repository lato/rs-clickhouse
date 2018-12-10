CREATE TABLE IF NOT EXISTS log1(
  source_ip     String,
  source_port   UInt16,
  dest_ip       String,
  dest_port     UInt16,
  query_id      UInt16,
  query_name    String,
  query_type    String,
  query_answer  String,
  query_result  Int8,
  is_query      UInt8,
  q_datetime    DateTime,
  q_ts          UInt64
)ENGINE = Log;
