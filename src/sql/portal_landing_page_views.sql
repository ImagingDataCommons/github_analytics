#standardSQL
#this query looks for the cohorts views from the portal.
#from the logs, cohort views are filtered by selecting only those rows which contain '/' in a 'GET' request. this table then acts as source of ips. 
#secondly locations are derived from ip addresses using a modified logic from 
#https://cloud.google.com/blog/products/data-analytics/geolocation-with-bigquery-de-identify-76-million-ip-addresses-in-20-seconds
#timestamp is rounded off to a minute to elimiate subsecond hits

INSERT INTO logs.portal_landing_page_views 

(
WITH
  sourceofips AS (
  SELECT
    DISTINCT
    DATETIME_TRUNC(`timestamp`, MINUTE) rounded_to_minute,
    httpRequest.remoteIp,
    httpRequest.requestUrl
  FROM
    `idc-admin-001.webapp_all_hits.appengine_googleapis_com_nginx_request_*`
  WHERE
 _TABLE_SUFFIX =FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AND
--  (_TABLE_SUFFIX between 
--  FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY)) AND  FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))) and 

    httpRequest.requestUrl IN ('/')
    AND httpRequest.requestMethod IN ('GET'))
SELECT
  rounded_to_minute,
  remoteIp,
  requestUrl,
  MIN(country_name) as country,
  MIN (subdivision_1_name) AS state,
  MIN(city_name) as city
FROM (
  SELECT
    *,
    NET.SAFE_IP_FROM_STRING(remoteIp) & NET.IP_NET_MASK(4,
      mask) network_bin
  FROM
    sourceofips,
    UNNEST(GENERATE_ARRAY(9,32)) mask
  WHERE
    BYTE_LENGTH(NET.SAFE_IP_FROM_STRING(remoteIp)) = 4 )
LEFT JOIN
#`fh-bigquery.geocode.201806_geolite2_city_ipv4_locs` --google deleted filipe hoffa's work, who wrote this query
  `idc-external-025.geolite2_20221205.geoip_city_v4`
USING
  (network_bin,
    mask)
GROUP BY
  rounded_to_minute,
  remoteIp,
  requestUrl
ORDER BY
  rounded_to_minute DESC 

) 