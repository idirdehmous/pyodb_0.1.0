SET $obstype=-1;
SET $varno=-1;
SET $press1=-1;
SET $press2=-1;
SET subtype1=-1;
SET subtype2=-1;

CREATE VIEW obsmon_conv
SELECT
obstype@hdr,codetype@hdr,statid,varno,lat@hdr,lon@hdr,vertco_type@body,vertco_reference_1@body,sensor@hdr,date,time@hdr,report_status.active@hdr,report_status.blacklisted@hdr,report_status.passive@hdr,report_status.rejected@hdr,datum_status.active@body,datum_status.blacklisted@body,datum_status.passive@body,datum_status.rejected@body,datum_anflag.final,an_depar,fg_depar,obsvalue,final_obs_error@errstat,biascorr_fg,lsm@modsurf
FROM hdr,body,modsurf,errstat WHERE
( obstype@hdr = $obstype ) AND
( varno@body = $varno ) AND
( codetype@hdr >= $subtype1 ) AND
( codetype@hdr <= $subtype2 ) AND
( vertco_reference_1@body > $press1 ) AND
( vertco_reference_1@body <= $press2 ) AND
( an_depar IS NOT NULL )
