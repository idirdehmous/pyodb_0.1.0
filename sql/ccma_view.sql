CREATE VIEW mandalay AS
SELECT
obstype,codetype, statid,varno,lat,lon,vertco_reference_1,vertco_reference_2 , date,time,obsvalue,fg_depar,an_depar
FROM  hdr,desc,body
SORT BY statid
