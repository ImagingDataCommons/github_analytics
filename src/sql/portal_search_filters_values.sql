#standardSQL
#this query looks for the search filter types and filter values from the portal.
#from the logs, after filtering for only those rows which contain '/explore/filters/?%' prefix, all filter types are extracted by a regular expression into an array. Similarly all corresponding filter values are extracted by another regular expression into an another array. Finally a struct enclosing these two arrays is made so that it can preserve the order while unnesting. this table then acts as source of ips. 
#secondly locations are derived from ip addresses using a modified logic from 
#https://cloud.google.com/blog/products/data-analytics/geolocation-with-bigquery-de-identify-76-million-ip-addresses-in-20-seconds

INSERT INTO logs.portal_search_filters_values ( 
with source_of_filters_and_ips
as (
SELECT
  timestamp,
  remoteIp,
  requestUrl,
  requestMethod,
  filter_type,
  filter_value 
FROM(  
SELECT
    timestamp,
    httpRequest.remoteIp,
    httpRequest.requestUrl,
    STRUCT(REGEXP_EXTRACT_ALL(httpRequest.requestUrl, r'([a-zA-Z_]+)=') AS filter_type,
      REGEXP_EXTRACT_ALL(httpRequest.requestUrl, r'=([a-zA-Z_\-0-9]+)')AS filter_value) FILTER,
    httpRequest.requestMethod
  FROM
    `idc-admin-001.webapp_all_hits.appengine_googleapis_com_nginx_request_*`
  WHERE


   (_TABLE_SUFFIX =FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))) AND

   -- (_TABLE_SUFFIX between 
  --FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY)) AND  FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))) and 
    httpRequest.requestUrl LIKE ('/explore/filters/?%')
    
    
    
    )


CROSS JOIN
  UNNEST(filter.filter_type) filter_type
WITH
OFFSET
  filter_type_array_order
LEFT JOIN
  UNNEST(filter.filter_value) filter_value
WITH
OFFSET
  filter_value_array_order
ON
  filter_type_array_order = filter_value_array_order #because unnesting destroys the order, offset is used to preserve the order and used as a reference while joining.
)
SELECT
  timestamp,
  requestUrl,
  requestMethod,
  filter_type,
  filter_value,
  remoteIp,
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
    source_of_filters_and_ips,
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
  requestMethod,
  filter_type,
  filter_value
ORDER BY
    timestamp DESC 

) 