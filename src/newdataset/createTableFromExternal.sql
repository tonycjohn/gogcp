CREATE TABLE
   RMS_22_PREAGG_US_6_PROD.trag_aggregated_data
 PARTITION BY
   RANGE_BUCKET(prd_id, GENERATE_ARRAY(863, 1123, 1))
   CLUSTER BY
   agd_fds_id
   AS
SELECT cast(REGEXP_EXTRACT(_FILE_NAME,r"(?:prd_id=)(\d+)") as int64) as prd_id,
cast(REGEXP_EXTRACT(_FILE_NAME,r"(?:agd_fds_id=)(\d+)") as int64) as agd_fds_id,
*
FROM RMS_22_PREAGG_US_6_PROD.EXT_trag_aggregated_data