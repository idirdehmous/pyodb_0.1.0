use strict;

my $where_fiddle = $ENV{'ODB_WHERE_FIDDLE'};

$where_fiddle = '$lat1 < degrees(lat) <= $lat2' if (!defined($where_fiddle));

{
  local $/;
  my $prtset = 0;
  my $in = <>;
  if ($in =~ m/\bwhere\b/i) {
    # WHERE-clause found
    $in =~ s/\b(where)\b\s*(\n)?/$1\n${where_fiddle} AND\n/i;
    $prtset = 1;
  }
  elsif ($in =~ m/\b(order|sort)\s*by\b/i) {
    # WHERE-clause is missing, but ORDERBY/SORTBY present
    $in =~ s/\b(order|sort)(\s*by)\b/WHERE ${where_fiddle}\n$1$2/i;
    $prtset = 1;
  }
  elsif ($in =~ m/(;)?\s*$/i) {
    # Both WHERE-clause and ORDERBY/SORTBY not present, try to add at the end
    $in =~ s/(\s*)(;)?\s*$/${1}WHERE ${where_fiddle}\n;/i;
    $prtset = 1;
  }
  if ($prtset) {
    print "SET \$lat1 = -90;\n";
    print "SET \$lat2 = +90;\n";
  }
  print "$in";
}

