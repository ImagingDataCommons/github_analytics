insert into logs.log_analysis

#standardSQL
#this query cleans up access logs first and stores into a temporary CTE and then 'INNER' joins
#corresponding dicom attributes with match on sop_instance_uid and then unions
#those logs that match on crdc_instance_uuid

#CTE
#searching storage access logs for 'GET' (CAN REMOVE THIS FILTER TO CHECK OTHER REQUESTS)
#requests with non empty cs_object strings,
WITH storage_access AS
(
 SELECT 
 distinct
time,
sc_status,
cs_method,
sc_bytes,
cs_bucket,
cs_object,
REGEXP_EXTRACT(cs_object,r'\/(.*)\.dcm') as extracted_uri
FROM `idc-admin-001.storage_access_prod.bucket_access_logs`  sap

where #SCANS ONLY FROM THE PREVIOUS DAY
#((DATE(time)> date(FORMAT_DATE("%Y-%m-%d", DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)))) and (DATE(time)<= date(FORMAT_DATE("%Y-%m-%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))))) 
time < '2024-01-18'

and
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
