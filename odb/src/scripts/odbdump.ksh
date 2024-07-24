#!/bin/ksh
#
# A utility to create statistics on odbdump-creation.
# Can be modified later on to create the actual dump(s)
#
# 26-Jun-2008 by Sami Saarinen, ECMWF
#

set -eu

set +v

pools=""
#pools="-p 1-2"

dbname=ECMA
odbdump=$ODB_FEBINPATH/odbdump.x

for f in ${dbname}.*.tar
do
  dir=$(basename $f .tar)

  echo "/database=$dir"

  dir=$(pwd)/$dir

  echo " "
  echo "    $(hostname):$dir"
  echo " "

  #-- Get analysis date & time, experiment name/version & 
  #   run date/time, actual no. of updates, no. of pools, no. of tables
  q0="select expver, andate, antime, creadate, creatime"
  q0="${q0}, mxup_traj, \$npools# as npools, \$ntables# as ntables from desc"
  $odbdump -i $dir -q "$q0" $pools -N -m1 | tee $dir.q0

  #-- Can this be a satellite at all ? 
  qsat="select max(sat.len) from hdr"
  $odbdump -i $dir -q "$qsat" $pools -N | tee $dir.qsat
  echo " "

  sat=$(egrep -v '^#' $dir.qsat | perl -pe 's/\s+//g;')
  if [[ $sat -gt 0 ]] ; then # This is a satellite
    grp="obstype,codetype,sensor"

    #-- Provide list of different satellite id's with description

    qid="select distinct $grp,satid,statid,satname[1:4] from hdr,sat"
    $odbdump -i $dir -q "$qid" $pools | tee $dir.qid
    echo " "

  else # conventional obs
    grp="obstype,codetype"

    #-- Provide list of different station id's/ident binned per obstype

    qid="select distinct $grp,satid,statid from hdr"
    $odbdump -i $dir -q "$qid" $pools | tee $dir.qid
    echo " "
  fi

  #-- Different groups: either per (obstype,codetype)-pair or (obstype,codetype,sensor)-triplet
  q1="select distinct $grp from hdr"
  $odbdump -i $dir -q "$q1" $pools -N -D, | tee $dir.q1
  echo " "

  #-- Count of hdr-entries binned by the group
  q2="select $grp,count(*) from hdr"
  $odbdump -i $dir -q "$q2" $pools -N | tee $dir.q2
  echo " "

  #-- Count of body-entries binned by the group AND variable number
  q3="select $grp,varno,count(*) from hdr,body"
  $odbdump -i $dir -q "$q3" $pools -N | tee $dir.q3
  echo " "

  #-- Get data volume statistics binned per group
  for w in $(egrep -v '^#' $dir.q1)
  do
    if [[ $sat -eq 1 ]] ; then
      where=$(echo "$w" | perl -pe 's/^\s*(\d+)/obstype=$1/;s/,(\d+)/ and codetype=$1/;s/,(\d+)/ and sensor=$1/;')
    else
      where=$(echo "$w" | perl -pe 's/^\s*(\d+)/obstype=$1/;s/,(\d+)/ and codetype=$1/;')
    fi    

    #-- Which tables present for this WHERE-statement ?

    tables="hdr,body,errstat"

    if [[ $sat -eq 1 ]] ; then
      for tbl in $(grep LINKLEN $dir/$dbname.dd | grep '@sat ' | perl -pe 's/^.*\(([^)].*)\).*/\1/')
      do
        query="select max(${tbl}.len) from hdr,sat where $where"
        cnt=$(set +e; $odbdump -i $dir -q "$query" $pools -T | perl -pe 's/\s+//g;')
        if [[ $cnt -gt 0 ]] ; then
          tables="${tables},$tbl"
          if [[ "$tbl" = "atovs" ]] ; then # automatically include atovs_pred
            tables="${tables},atovs_pred"
          fi
          #-- search for possible ${tbl}_body -tables
          tbl_body="${tbl}_body"
          query="select max(${tbl_body}.len) from hdr,sat,$tbl where $where"
          cnt=$(set +e; $odbdump -i $dir -q "$query" $pools -T | perl -pe 's/\s+//g;')
          if [[ $cnt -gt 0 ]] ; then
            tables="${tables},$tbl_body"
          fi
        fi
      done
    fi

    qstat_suffix=$(echo "$w" | perl -pe 's/^\s*/group=/; s/\s+/_/g; s/\s+/_/g; s/_$//;')

    cols='lldegrees(lat@hdr) as lat@hdr, lldegrees(lon@hdr) as lon@hdr, "~/(LINK|:lat@hdr|:lon@hdr)/"'

    hash_tables=$(echo "$tables" | perl -pe 's/(\w+)/#$1/g; s/,\s*$//')
    qstat="select \$pool#, $hash_tables , $cols from $tables where $where"
    $odbdump -i $dir -q "$qstat" $pools -s | tee $dir.qstat.$qstat_suffix
    echo " "
  done # for w in $(egrep -v '^#' $dir.q1)

  echo "/end"
done
