#standardSQL
#this query looks for the cohorts created from the portal.
#from the logs, cohort id is extracted by a regular expression after filtering for only those rows which contain '/cohorts/' prefix and followed by an integer and rows only containing 3 forward slashes. It then selects the first time a cohort id is created. this table then acts as source of ips. 
#secondly locations are derived from ip addresses using a modified logic from 
#https://cloud.google.com/blog/products/data-analytics/geolocation-with-bigquery-de-identify-76-million-ip-addresses-in-20-seconds
insert into logs.portal_cohorts_created 
(
WITH
  sourceofips AS (
  SELECT
    min (timestamp) AS firsttimestamp,
    httpRequest.remoteIp,
    httpRequest.requestUrl,
    REGEXP_EXTRACT(httpRequest.requestUrl,r'/.*?/(.*?)/') AS cohort_id
  FROM
    `idc-admin-001.webapp_all_hits.appengine_googleapis_com_nginx_request_*`
  WHERE
  (_TABLE_SUFFIX = FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)))
  
  and

    httpRequest.requestUrl LIKE ('%/cohorts/%') AND
    (SAFE_CAST(REGEXP_EXTRACT(httpRequest.requestUrl,r'/.*?/(.*?)/') AS int64) IS NOT NULL)
    AND ARRAY_LENGTH(REGEXP_EXTRACT_ALL(httpRequest.requestUrl, r'/'))=3
  GROUP BY
   httpRequest.remoteIp,
   httpRequest.requestUrl,
   cohort_id
  )
SELECT 
firsttimestamp,
remoteIp,
requestUrl,
cohort_id,
MIN(country_name) as country,
MIN (subdivision_1_name) as state,
MIN(city_name) as city
from
(
  SELECT 
    *,
    NET.SAFE_IP_FROM_STRING(remoteIp) & NET.IP_NET_MASK(4,
      mask) network_bin
  FROM
    sourceofips,
    UNNEST(GENERATE_ARRAY(9,32)) mask
  WHERE
    BYTE_LENGTH(NET.SAFE_IP_FROM_STRING(remoteIp)) = 4 )
LEFT join 
  #`fh-bigquery.geocode.201806_geolite2_city_ipv4_locs` --google deleted filipe hoffa's work, who wrote this query
  `idc-external-025.geolite2_20221205.geoip_city_v4`
USING
  (network_bin,
   mask)
GROUP BY
firsttimestamp,
remoteIp,
requestUrl,
cohort_id
ORDER BY
    firsttimestamp DESC,
    cohort_id DESC

)