#standardSQL
insert into logs.ohif_slim_viewer_hits 
#this query is broken down to two CTEs
#first extracts studyinstanceuids from slim and ohif viewer url hits on web, along with their corresponding metadata like
#httpstatus, referer,remoteip and then joins the extracted studyinstanceuids with studyinstanceuids from auxilary metadata table in idc_current dataset
#note timestamp is rounded to nearest minute to remove duplicate hits within a fraction of seconds
#ip 170.223.207.1 is 1249 boylston st and filtered against it
#secondly locations are derived from ip addresses using a modified logic from 
#https://cloud.google.com/blog/products/data-analytics/geolocation-with-bigquery-de-identify-76-million-ip-addresses-in-20-seconds

WITH
  sourceofips AS (
  SELECT
    DISTINCT DATETIME_TRUNC(`timestamp`, MINUTE) rounded_to_minute,
    httpRequest.requestUrl,
    CASE
      WHEN REGEXP_EXTRACT(httpRequest.requestUrl,r'([a-z]{4,6}[\/])')='slim/' THEN 'slim'
      WHEN REGEXP_EXTRACT(httpRequest.requestUrl,r'([a-z]{4,6}[\/])')='viewer/' THEN 'ohif'
  END
    AS viewer_type,
    CASE
      WHEN REGEXP_EXTRACT(httpRequest.requestUrl,r'([a-z]{4,6}[\/])')='slim/' THEN CONCAT('https://viewer.imaging.datacommons.cancer.gov/slim/studies/', REGEXP_EXTRACT(httpRequest.requestUrl, r'([^=a-z\/]+[0-9\.])'))
      WHEN REGEXP_EXTRACT(httpRequest.requestUrl,r'([a-z]{4,6}[\/])')='viewer/' THEN CONCAT('https://viewer.imaging.datacommons.cancer.gov/v3/viewer/?StudyInstanceUIDs=',REGEXP_EXTRACT(httpRequest.requestUrl, r'([^=a-z\/]+[0-9\.])'))
  END
    AS generated_study_url,
    REGEXP_EXTRACT(httpRequest.requestUrl, r'([^=a-z\/]+[0-9\.])') AS extracted_url,
    httpRequest.status,
    httpRequest.remoteIp,
    httpRequest.referer,
    collection_name,
    StudyInstanceUID,
    submitter_case_id
  FROM
    `idc-admin-001.viewer_all_hits.requests_*` viewer
  JOIN
    `bigquery-public-data.idc_current.auxiliary_metadata` metadata
  ON
    REGEXP_EXTRACT(viewer.httpRequest.requestUrl, r'([^=a-z\/]+[0-9\.])') =metadata.StudyInstanceUID
  WHERE

  (_TABLE_SUFFIX =FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))) and
    ( httpRequest.requestUrl LIKE ('https://viewer.imaging.datacommons.cancer.gov/viewer/%')
      OR httpRequest.requestUrl LIKE ('https://viewer.imaging.datacommons.cancer.gov/v3/viewer/%')
      OR httpRequest.requestUrl LIKE ('https://viewer.imaging.datacommons.cancer.gov/slim/studies/%') ) )
SELECT
  rounded_to_minute,
  viewer_type,
  requestUrl,
  extracted_url,
  generated_study_url,
  status,
  collection_name,
  StudyInstanceUID,
  submitter_case_id,
  remoteIp,
  country_name,
  city_name,
  postal_code
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
JOIN
  #`fh-bigquery.geocode.201806_geolite2_city_ipv4_locs` --google deleted filipe hoffa's work, who wrote this query
  `idc-external-025.geolite2_20221205.geoip_city_v4`
USING
  (network_bin,
    mask)
