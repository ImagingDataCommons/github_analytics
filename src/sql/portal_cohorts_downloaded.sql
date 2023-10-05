#standardSQL
#this query looks for the cohorts downloaded from the portal.
#from the logs, cohort id is extracted by a regular expression after filtering for only those rows which contain '/cohorts/' prefix and rows only containing 'download_manifest'.this table then acts as source of ips. 
#secondly locations are derived from ip addresses using a modified logic from 
#https://cloud.google.com/blog/products/data-analytics/geolocation-with-bigquery-de-identify-76-million-ip-addresses-in-20-seconds

INSERT INTO logs.portal_cohorts_downloaded 
(
WITH
  sourceofips AS (
    SELECT
    timestamp,
    httpRequest.remoteIp,
    httpRequest.requestUrl,
    httpRequest.responseSize,
   case #this checks if cohort id is present in ids="xxxx" string or download_manifest/'xxxx'/
        when regexp_extract(httpRequest.requestUrl,'.+?ids=([0-9]+)') is null then regexp_extract(httpRequest.requestUrl,'/.*?/.*?/([0-9]+)')
        else regexp_extract(httpRequest.requestUrl,'.+?ids=([0-9]+)') end as cohort_id

  FROM
    `idc-admin-001.webapp_all_hits.appengine_googleapis_com_nginx_request_*`
  WHERE
 (_TABLE_SUFFIX= FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))) and 


    httpRequest.requestUrl LIKE ('%/cohorts/%') AND
    REGEXP_EXTRACT(httpRequest.requestUrl,r'/.*?/(.*?)/') in ('download_manifest')#extracts and checks for the keyword download_manifest
)
SELECT 
timestamp,
remoteIp,
requestUrl,
responseSize,
cohort_id,
MIN(country_name) as country,
MIN (subdivision_1_name) as state,
MIN(city_name) as city
FROM
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
timestamp,
remoteIp,
requestUrl,
responseSize,
cohort_id
ORDER BY
    timestamp DESC

)  