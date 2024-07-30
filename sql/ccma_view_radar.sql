CREATE VIEW radar_view AS
SELECT varno,lat,lon,obsvalue FROM  hdr,desc,body where obstype==13 
