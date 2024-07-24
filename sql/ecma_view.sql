CREATE VIEW odb_view AS
SELECT obstype,codetype,statid,varno,lat,lon,vertco_reference_1,date,time,obsvalue
FROM  hdr,desc,body SORT BY statid
