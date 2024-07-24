CREATE VIEW radar_view AS
SELECT varno,lat,lon,obsvalue FROM  hdr,desc,body where varno == 192 SORT BY statid 
