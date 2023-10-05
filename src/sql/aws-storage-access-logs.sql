-- Query Overview:
-- This SQL query enriches AWS S3 access logs with location information (country name, city name, postal code)
-- based on the IP addresses found in the logs. It performs the following steps:

-- Step 1: Extract relevant fields from AWS S3 access logs
-- - Three S3 access log tables are combined using UNION ALL.
-- - The query filters records where the operation is 'REST.GET.OBJECT' and the HTTP status is '200' or '206'.

-- Step 2: Extract all unique IPs from AWS S3 access logs

-- Step 3: Generate masks and network bins from the unique IPs
-- - IP addresses are converted to network bins by applying masks using bitwise operations.

-- Step 4: Left join on geoip_city_v4 to get locations for available IPs
-- - IP address mappings are obtained from the 'geoip_city_v4' table using the generated masks.

-- Final Query: Combine AWS logs with location information
-- - The original S3 access logs are left-joined with the IP address mappings to get the complete information.
-- - The resulting dataset includes time, bucket, remote_ip, requester, http_status, operation, object_size,
--   bytes_sent, dicom_file_name, key, request_uri, country_name, city_name, and postal_code fields.

-- This query ensures that all records from the original 'aws_bucket_access_logs' are retained while obtaining
-- location information for each IP address based on the generated masks. If location information is not
-- available for certain IP addresses, the corresponding fields will be NULL in the final result.


-- Step 1: Extract relevant fields from AWS S3 access logs
CREATE OR REPLACE TABLE `idc-external-025.logs.aws-storage-access-logs` as
(
WITH aws_bucket_access_logs AS (
  SELECT
    time,
    bucket,
    remote_ip,
    http_status,
    operation,
    object_size,
    bytes_sent,
    REGEXP_EXTRACT(request_uri, r'([^\/]+\.dcm)\b') AS dicom_file_name,
    request_uri
  FROM
    `idc-admin-001.aws_logs.s3_access_pub` aws_pub
  WHERE
    operation ='REST.GET.OBJECT'
    AND http_status IN ('200', '206')
  UNION ALL
  SELECT
    time,
    bucket,
    remote_ip,
    http_status,
    operation,
    object_size,
    bytes_sent,
    REGEXP_EXTRACT(request_uri, r'([^\/]+\.dcm)\b') AS dicom_file_name,
    request_uri
  FROM
    `idc-admin-001.aws_logs.s3_access_pub_cr` aws_cr
  WHERE
    operation ='REST.GET.OBJECT'
    AND http_status IN ('200', '206')
  UNION ALL
  SELECT
    time,
    bucket,
    remote_ip,
    http_status,
    operation,
    object_size,
    bytes_sent,
    REGEXP_EXTRACT(request_uri, r'([^\/]+\.dcm)\b') AS dicom_file_name,
    request_uri
  FROM
    `idc-admin-001.aws_logs.s3_access_pub_two` aws_cr
  WHERE
    operation ='REST.GET.OBJECT'
    AND http_status IN ('200', '206')
),

-- Step 2: Extract all unique IPs from AWS S3 access logs
unique_ips AS (
  SELECT DISTINCT remote_ip
  FROM aws_bucket_access_logs
),

-- Step 3: Generate masks and network bins from the unique IPs
ip_masks AS (
  SELECT
    remote_ip,
    mask,
    NET.SAFE_IP_FROM_STRING(remote_ip) & NET.IP_NET_MASK(4, mask) AS network_bin
  FROM
    unique_ips
  CROSS JOIN
    UNNEST(GENERATE_ARRAY(9, 32)) mask
  WHERE
    BYTE_LENGTH(NET.SAFE_IP_FROM_STRING(remote_ip)) = 4
),

-- Step 4: Left join on geoip_city_v4 to get locations for available IPs
ip_address_mappings AS (
  SELECT
    mask,
    remote_ip,
    network_bin,
    country_name,
    city_name,
    postal_code
  FROM
    ip_masks
  JOIN
    `idc-external-025.geolite2_20221205.geoip_city_v4`
  USING (network_bin, mask)
),

aws_idc_urls AS

(SELECT
collection_id,
StudyDescription,
StudyInstanceUID,
SeriesInstanceUID,
SOPInstanceUID,
Modality,
REGEXP_EXTRACT(aws_url, r'\/([^\/]*)$') as aws_file_name

from 
`bigquery-public-data.idc_current.dicom_all` idc 

)

-- Final query: Combine AWS logs with location information
SELECT
  time,
  collection_id,
StudyDescription,
StudyInstanceUID,
SeriesInstanceUID,
SOPInstanceUID,
Modality,
  bucket,
  aws_bucket_access_logs.remote_ip,
  http_status,
  operation,
  object_size,
  bytes_sent,
  dicom_file_name,
  request_uri,
  country_name,
  city_name,
  postal_code
FROM
  aws_bucket_access_logs
LEFT JOIN
  ip_address_mappings
USING (remote_ip)
JOIN aws_idc_urls on 

aws_bucket_access_logs.dicom_file_name= aws_idc_urls.aws_file_name


)