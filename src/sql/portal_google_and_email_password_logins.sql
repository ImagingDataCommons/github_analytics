#standardSQL
#this query looks for the portal logins
#the query is broken down to two parts.
#Part 1
# Logins are first obtained by filtering for those rows satisfying the following conditions
#for google(gmail) based logins, a request url like '/accounts/google/login/callback/%' is usually followed by '/explore/' and in some cases 
#'/users/%' and '/cohorts/%' with in 3 seconds. Both conditions use 'GET' request method. This pattern is assumed as a successful login.
#Similarly for email/password logins, a request url like '/accounts/login/' with a POST request method is usually followed by '/explore/' and in some cases #'/users/%' and '/cohorts/%' with a 'GET' request in 3 seconds. This pattern is also assumed as a successful login.
#the idea is to use a 'lead' window function to get the value in the next row after partitioning by ipaddress and timestamp rounded to the nearest minute
#Part 2
#secondly locations are derived from ip addresses using a modified logic from 
#https://cloud.google.com/blog/products/data-analytics/geolocation-with-bigquery-de-identify-76-million-ip-addresses-in-20-seconds


INSERT INTO logs.portal_google_and_email_password_logins 
(
WITH
  source_of_ips_and_logins AS (
  SELECT
    httpRequest.remoteIp,
    httpRequest.requestMethod,
    LEAD(httpRequest.requestMethod) OVER (PARTITION BY DATETIME_TRUNC(`timestamp`, MINUTE),
      httpRequest.remoteIp
    ORDER BY
      timestamp ) leadingRowRequestMethod,#window function lead is used to get the value in the next row  
    timestamp,
    LEAD(timestamp) OVER (PARTITION BY DATETIME_TRUNC(`timestamp`, MINUTE),
      httpRequest.remoteIp
    ORDER BY
      timestamp )leadingRowtimestamp,
    httpRequest.requestUrl,
    LEAD(httpRequest.requestUrl) OVER (PARTITION BY DATETIME_TRUNC(`timestamp`, MINUTE),
      httpRequest.remoteIp
    ORDER BY
      timestamp )leadingRowrequestUrl,
  FROM
    `idc-admin-001.webapp_all_hits.appengine_googleapis_com_nginx_request_*`
  WHERE
 _TABLE_SUFFIX = FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AND
 -- (_TABLE_SUFFIX between FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY)) AND  FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))) AND

    ((((httpRequest.requestUrl IN ('/explore/')--
          OR httpRequest.requestUrl LIKE ('/cohorts/%')--email
          OR httpRequest.requestUrl LIKE ('/users/%'))--password
        AND httpRequest.requestMethod IN ('GET'))--login
      OR (httpRequest.requestUrl IN ('/accounts/login/')--conditions
        AND httpRequest.requestMethod IN ('POST')))--
    OR ((httpRequest.requestUrl IN ('/explore/')#
        OR httpRequest.requestUrl LIKE ('/cohorts/%')#google
        OR httpRequest.requestUrl LIKE ('/users/%')#gmail
        OR httpRequest.requestUrl LIKE ('/accounts/google/login/callback/%'))#login 
      AND httpRequest.requestMethod IN ('GET')))#conditions
  ORDER BY
    remoteIp,
    timestamp DESC )
SELECT
  timestamp,
  remoteIp,
  MIN(country_name) AS country,
  MIN (subdivision_1_name) AS state,
  MIN(city_name) AS city,
  requestMethod,
  leadingRowRequestMethod,
  leadingRowtimestamp,
  requestUrl,
  leadingRowrequestUrl,
  TIME_DIFF( time (leadingRowtimestamp), TIME(timestamp), second) AS latency,
FROM (
  SELECT
    *,
    NET.SAFE_IP_FROM_STRING(remoteIp) & NET.IP_NET_MASK(4,
      mask) network_bin
  FROM
    source_of_ips_and_logins,
    UNNEST(GENERATE_ARRAY(9,32)) mask
  WHERE
    BYTE_LENGTH(NET.SAFE_IP_FROM_STRING(remoteIp)) = 4 )
LEFT JOIN
#`fh-bigquery.geocode.201806_geolite2_city_ipv4_locs` --google deleted filipe hoffa's work, who wrote this query
  `idc-external-025.geolite2_20221205.geoip_city_v4`
USING
  (network_bin,
    mask)
WHERE
  ((requestMethod IN ('POST')
      AND leadingRowRequestMethod IN ('GET')
      AND requestUrl IN ('/accounts/login/')
      AND (leadingRowrequestUrl IN ('/explore/')
        OR leadingRowrequestUrl LIKE ('/cohorts/%')
        OR leadingRowrequestUrl LIKE ('/users/%')))
    OR (requestMethod IN ('GET')
      AND requestUrl LIKE ('/accounts/google/login/callback/%')
      AND (leadingRowrequestUrl IN ('/explore/')
        OR leadingRowrequestUrl LIKE ('/cohorts/%')
        OR leadingRowrequestUrl LIKE ('/users/%'))))
  AND (TIME_DIFF( time (leadingRowtimestamp), TIME(timestamp), second)<=3)
GROUP BY
  remoteIp,
  requestMethod,
  leadingRowRequestMethod,
  timestamp,
  leadingRowtimestamp,
  requestUrl,
  leadingRowrequestUrl,
  latency
ORDER BY
  timestamp desc

)