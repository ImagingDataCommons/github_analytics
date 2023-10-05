-- Query Overview:
-- This SQL query enriches IDC API access logs with location information (country name, city name, postal code)
-- based on the IP addresses found in the logs. It performs the following steps:

-- Step 1: Extract relevant fields from IDC API access logs
-- - The API access log table 'idc-admin-001.api_all_hits.appengine_googleapis_com_nginx_request' is queried.
-- - The query filters records with HTTP status code '200' and URLs that match the '/v1/' pattern.

-- Step 2: Extract all unique IPs from IDC API access logs
-- - Unique IP addresses are selected from the extracted API access logs.

-- Step 3: Generate masks and network bins from the unique IPs
-- - IP addresses are converted to network bins by applying masks using bitwise operations.

-- Step 4: Left join on geoip_city_v4 to get locations for available IPs
-- - IP address mappings are obtained from the 'geoip_city_v4' table using the generated masks.

-- Final Query: Combine IDC API logs with location information
-- - The original API access logs are left-joined with the IP address mappings to get the complete information.
-- - The resulting dataset includes timestamp, requestMethod, status, requestUrl, remoteIp, apiRequestCategory,
--   isPaginatedResult, country_name, city_name, and postal_code fields.
-- - Records with NULL apiRequestCategory are filtered out.

-- Step 1: Extract relevant fields from IDC API access logs
insert into `idc-external-025.logs.idc_api_logs`
(WITH idc_api_logs AS (
  SELECT
    timestamp,
    httpRequest.requestMethod,
    httpRequest.status,
    httpRequest.requestUrl,
    httpRequest.remoteIp,
    CASE
      -- Categorize API requests based on their request URLs
      WHEN REGEXP_CONTAINS(httpRequest.requestUrl, r'^/v[0-9]+/about') THEN 'about'
      WHEN REGEXP_CONTAINS(httpRequest.requestUrl, r'^/v[0-9]+/(versions|collections|analysis_results|attributes)') THEN 'dataModel'
      WHEN REGEXP_CONTAINS(httpRequest.requestUrl, r'^/v[0-9]+/cohorts') THEN 'cohorts'
      WHEN REGEXP_CONTAINS(httpRequest.requestUrl, r'^/v[0-9]+/cohorts/manifest') THEN 'manifests'
      WHEN REGEXP_CONTAINS(httpRequest.requestUrl, r'^/v[0-9]+/cohorts/query') THEN 'queries'
      WHEN REGEXP_CONTAINS(httpRequest.requestUrl, r'^/v[0-9]+/dicomMetadata') THEN 'queries' -- Modify the category name as needed
      WHEN REGEXP_CONTAINS(httpRequest.requestUrl, r'^/v[0-9]+/users') THEN 'users'
      ELSE NULL -- Handle cases not covered by the defined conditions
    END AS apiRequestCategory,
    CASE WHEN httpRequest.requestUrl LIKE '%nextPage%' THEN 'paginatedResult'
         ELSE 'notApplicable'
    END AS isPaginatedResult
  FROM
    `idc-admin-001.api_all_hits.appengine_googleapis_com_nginx_request`
  WHERE
    REGEXP_CONTAINS(httpRequest.requestUrl, r'^/v[0-9]+/') -- Filter for API requests only
    AND NOT REGEXP_CONTAINS(httpRequest.requestUrl, r'^/v[0-9]+/swagger') -- Exclude Swagger requests
    AND httpRequest.status = 200 -- Filter successful requests
),

-- Step 2: Extract all unique IPs from IDC API access logs
unique_ips AS (
  SELECT DISTINCT remoteIp
  FROM idc_api_logs
),

-- Step 3: Generate masks and network bins from the unique IPs
ip_masks AS (
  SELECT
    remoteIp,
    mask,
    NET.SAFE_IP_FROM_STRING(remoteIp) & NET.IP_NET_MASK(4, mask) AS network_bin
  FROM
    unique_ips
  CROSS JOIN
    UNNEST(GENERATE_ARRAY(9, 32)) mask
  WHERE
    BYTE_LENGTH(NET.SAFE_IP_FROM_STRING(remoteIp)) = 4
),

-- Step 4: Left join on geoip_city_v4 to get locations for available IPs
ip_address_mappings AS (
  SELECT
    mask,
    remoteIp,
    network_bin,
    country_name,
    city_name,
    postal_code
  FROM
    ip_masks
  JOIN
    `idc-external-025.geolite2_20221205.geoip_city_v4`
  USING (network_bin, mask)
)

-- Final query: Combine IDC API logs with location information
SELECT
  timestamp,
  requestMethod,
  status,
  requestUrl,
  idc_api_logs.remoteIp,
  apiRequestCategory,
  isPaginatedResult,
  country_name,
  city_name,
  postal_code
FROM
  idc_api_logs
LEFT JOIN
  ip_address_mappings
USING (remoteIp)
WHERE
  apiRequestCategory IS NOT NULL -- Filter out records with NULL categories
AND  
TIMESTAMP_TRUNC(timestamp, DAY) = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
ORDER BY timestamp desc
)