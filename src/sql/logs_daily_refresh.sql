insert into logs.log_analysis

#standardSQL
#this query cleans up access logs first and stores into a temporary CTE and then 'INNER' joins
#corresponding dicom attributes with match on sop_instance_uid and then unions
#those logs that match on crdc_instance_uuid

#CTE
#searching storage access logs for 'GET' (CAN REMOVE THIS FILTER TO CHECK OTHER REQUESTS)
#requests with non empty cs_object strings,
WITH storage_access AS
(SELECT 
time,
sc_status,
cs_method,
sc_bytes,
cs_bucket,
cs_object,
#whenever there is a dash in cs_object,remove only '.dcm' from the end of the string
#when there is no dash in the sop_instance_uid part of the cs_object,remove  '.dcm' from the sop_instance_uid part of the string
#when there is no match to above two cases,returns cs_object
#REGEXP_EXTRACT(cs_uri,r'([^F]+.dcm)') extracts sop_instance_uid from the cs_uri column, and then REGEXP_CONTAINS checks for '-' (dash)

case when not REGEXP_CONTAINS(REGEXP_EXTRACT(cs_object,r'[^/]+dcm$'),'[\\-]') then REGEXP_REPLACE(REGEXP_EXTRACT(cs_object,r'[^/]+dcm$'),'.dcm','')
     --when not REGEXP_CONTAINS(REGEXP_EXTRACT(cs_uri,r'([^F]+.dcm)'),'[\\-]') then REGEXP_REPLACE(REGEXP_EXTRACT(cs_uri,r'([^F]+.dcm)'),'.dcm','')
     when REGEXP_CONTAINS(cs_object,r'[\\-]') then REGEXP_EXTRACT(cs_object,r'(.*)\.')
     else cs_object
end as extracted_uri
FROM `idc-admin-001.storage_access_prod.bucket_access_logs`  sap
where #SCANS ONLY FROM THE PREVIOUS DAY
((DATE(time)> date(FORMAT_DATE("%Y-%m-%d", DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)))) and (DATE(time)<= date(FORMAT_DATE("%Y-%m-%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))))) and
cs_object IS NOT NULL AND 
cs_object NOT in ('') 
)
SELECT 
time,
sc_status,
cs_method,
sc_bytes,
cs_bucket,
cs_object,
extracted_uri,
collection_id,
StudyDescription,
StudyInstanceUID,
SeriesInstanceUID,
SOPInstanceUID,
crdc_instance_uuid 
from storage_access sa

join `bigquery-public-data.idc_current.dicom_all` da on sa.extracted_uri =da.SOPInstanceUID
where collection_id is not null

UNION DISTINCT

select 
time,
sc_status,
cs_method,
sc_bytes,
cs_bucket,
cs_object,
extracted_uri,
collection_id,
StudyDescription,
StudyInstanceUID,
SeriesInstanceUID,
SOPInstanceUID,
crdc_instance_uuid 
from storage_access sa
join `bigquery-public-data.idc_current.dicom_all` da on sa.extracted_uri =da.crdc_instance_uuid 
where collection_id is not null

ORDER BY TIME DESC