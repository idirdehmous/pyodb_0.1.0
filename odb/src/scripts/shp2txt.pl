#
# A Perl-script that converts ARCgis shapelib's closed polygon regions ("rings") into
# a text file for use by world2odb.c
#
# Auxiliary utilities needed : (available via shapelib-1.2.10 public domain source code)
#
#   dbfdump
#   shpdump
#
# Usage: 
# 1) To print just the available columns:
#  echo "some_shapelib_directory" | perl -w sh2txt.pl 
# 2) To use ';'-delimited name out of columns par1 par2 par3, use:
#  echo "shapelib-directory par1 par2 par3" | perl -w sh2txt.pl 
# This will also create a text-file `basename some_shapelib_directory`.txt for world2odb.x
#
# Author: Sami Saarinen, ECMWF, 24-Jul-2007
#

use strict;

my $inp = <>; # echo "some_shapelib_directory [param(s)]";
my ($db,@par) = split(/\s+/, $inp);
print STDERR "ARCgis/shapelib database='$db'\n";
my $npars = 0;
for (@par) {
    my $par = $_;
    print STDERR "\$par='$par'\n";
    $npars++;
}

open(DBF, "dbfdump -h $db|") || die "Unable to dbfdump -h $db";
my @dbfdump_cols = <DBF>;
close(DBF);
my $ncols = 0;
my $nrows = -1;
for (@dbfdump_cols) {
    chomp;
    next if (m/^\s*$/);
    if (m/^Field\s+(\d+): Type=(\w+), Title=\`(.*)\', Width=(\d+), Decimals=(\d+)/) {
	my $field = $1;
	my $type = $2;
	my $title = $3;
	my $width = $4;
	my $dec = $5;
	print STDERR "$field\t$type\t'$title'\t$width\t$dec\n";
	$ncols++;
    }
    else {
	$nrows++;
    }
}
print STDERR "ncols = $ncols, nrows = $nrows\n";

exit if ($npars == 0);

open(SHP, "shpdump $db|") || die "Unable to shpdump $db";
my @shpdump = <SHP>;
close(SHP);

my $nshapes = 0;
my $bounds = 0;
my ($fileminlon, $fileminlat, $filemaxlon, $filemaxlat) = (0, 0, 0, 0);

for (@shpdump) {
    chomp;
    next if (m/^\s*$/);
    if (m/^Shapefile Type: Polygon   # of Shapes:\s*(\d+)/) {
	$nshapes = $1;
    }
    elsif ($nshapes > 0 && $bounds == 0 && m/^File Bounds:/) {
	$bounds++;
	s/[(),]/ /g; s/^\s+//;
	my $dummy;
	($dummy, $dummy, $fileminlon, $fileminlat) = split(/\s+/);
    }
    elsif ($nshapes > 0 && $bounds == 1 && m/^\s+to\s+/) {
	$bounds++;
	s/[(),]/ /g; s/^\s+//;
	my $dummy;
	($dummy, $filemaxlon, $filemaxlat) = split(/\s+/);
	last;
    }
}

print STDERR "nshapes = $nshapes\n";
print STDERR "File min (lat,lon) : $fileminlat, $fileminlon\n";
print STDERR "File max (lat,lon) : $filemaxlat, $filemaxlon\n";

die "No. of rows ($nrows) <> no. of shapes ($nshapes)" if ($nrows != $nshapes);

my $out = "$db";
$out =~ s|.*/||;
$out =~ s|[.].*$||;
$out .= ".txt";
print STDERR "out='$out'\n";
open(OUT,"> $out") || die "Unable to open file '$out' for writing";

printf(OUT "# Number of recs %d\n", $nrows);
printf(OUT "# Shapelib database: %s\n",$db);
printf(OUT "# Name consists of these %d columns: (", $npars);
for (@par) {
    my $par = $_;
    printf(OUT "%s%s",$par,(--$npars > 0) ? "," : ")\n");
}
printf(OUT "# Global box: %.3f %.3f %.3f %.3f\n",
	$fileminlon, $fileminlat, $filemaxlon, $filemaxlat);
printf(OUT "# Shp information:\n");
printf(OUT "# Part\n");

open(DBF, "dbfdump -m $db|") || die "Unable to dbfdump -m $db";
my @dbfdump_rec = <DBF>;
close(DBF);
my $recno = -1;
my $last = "";
for (@dbfdump_rec) {
    chomp;
    next if (m/^\s*$/);
    if (m/^Record:\s+(\d+)/) {
	$recno = "$1";
	if ($last !~ m/^\s*$/) {
	    $last =~ s/;$//;
	    print STDERR "$last\n";
	    &ReadShape(split(/\t/,$last));
	}
	$last = "$recno\t";
    }
    else {
	s/\s+$//;
	my $this = $_;
	for (@par) {
	    my $par = $_;
	    $last .= "$1;" if ($this =~ /^$par:\s*(.*)\s*$/i);
	}
    }
}

if ($last !~ m/^\s*$/) {
    $last =~ s/;$//;
    print STDERR "$last\n";
    &ReadShape(split(/\t/,$last));
}

close(OUT);

exit(0);

sub ReadShape {
    my ($shapeno, $name) = @_;
    print STDERR "--> shapeno=$shapeno (out of $nshapes), name='$name'\n";
    printf(OUT "# Name: %s\n",$name);
    my ($nvert, $nparts, $partno) = (0, 0, 0);
    my $jv = 0;
    my $bounds = 0;
    my ($minlon, $minlat, $maxlon, $maxlat) = (0, 0, 0, 0);
    for (@shpdump) {
	chomp;
	next if (m/^\s*$/);
	s/[(),=]/ /g; s/^\s+//; s/\s+/ /g;
	if (m/^Shape:\s*(\d+) Polygon nVertices (\d+) nParts (\d+)/) {
	    if ($1 == $shapeno) {
		$nvert = $2;
		$nparts = $3;
	    }
	}
	elsif ($nvert > 0 && $bounds == 0 && m/^Bounds:/) {
	    $bounds++;
	    my $dummy;
	    ($dummy, $minlon, $minlat) = split(/\s+/);
	}
	elsif ($nvert > 0 && $bounds == 1 && m/^to\s+/) {
	    $bounds++;
	    my $dummy;
	    ($dummy, $maxlon, $maxlat) = split(/\s+/);
	}
	elsif ($nvert > 0 && $bounds == 2) {
	    if ($jv == 0) {
		print STDERR "\tnvert=$nvert, nparts=$nparts\n";
		printf(STDERR "# Box: %.3f %.3f %.3f %.3f\n",$minlon, $minlat, $maxlon, $maxlat);
		printf(OUT "# Num of parts: %d\n",$nparts);
		printf(OUT "# Num of points: %d\n",$nvert);
		printf(OUT "# Box: %.3f %.3f %.3f %.3f\n",$minlon, $minlat, $maxlon, $maxlat);
		printf(OUT "# Part %d\n",$partno++);
	    }
	    if (m/^[+]\s+/) {
		printf(OUT "# Part %d\n",$partno++);
		s/^[+]\s+//;
	    }
	    my ($lat, $lon) = split(/\s+/);
	    printf(OUT "%.3f %.3f\n", $lat, $lon);
	    $jv++;
	    last if ($jv == $nvert);
	}
    } # for (@shpdump)
}
