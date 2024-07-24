#
# dcafix.pl
#

use strict;

# Read environment variables
my $datapath = $ENV{'DATAPATH'} || ".";

my @in = <>;
my @out = ();

my $dcaline = "";
for (@in) {
  chomp;
  if (m/^\d+/) {
    push(@out,$_);
  }
  elsif (m/^\#DCA:/) {
    $dcaline = $_;
  }
}

if ($dcaline =~ m/^\#DCA:/) {
  my $is_hcat = 0;
  my $file_tested = 0;
  my $maxfilelen = 0;

  for (@out) {
    my ($col, $colname, $dtname, $dtnum, $file, $poolno, $offset, $length,
	$pmethod, $pmethod_actual, $nrows, $nmdis, 
	$avg, $min, $max, $res1, $res2, $is_little) = split(/\s+/);
    my $len = length($file);
    $maxfilelen = $len if ($len > $maxfilelen);
    if (!$file_tested) {
      $file = "$datapath/$file" if (! -f $file);
      if (-f $file) { # quick'n'dirty
	my $word = `od -cv $file|head -1`;
	$word =~ s/^\s*\S+\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+.*$/$1$2$3$4/;
	chomp($word);
	$is_hcat = ($word eq "HC32" || $word eq "23CH");
      }
      $file_tested = 1;
    }
    last if (!$is_hcat);
  }

  if ($is_hcat) {
    my $key;
    my %keyval = ();
    for (@out) {
      my ($col, $colname, $dtname, $dtnum, $file, $poolno, $offset, $length,
	  $pmethod, $pmethod_actual, $nrows, $nmdis, 
	  $avg, $min, $max, $res1, $res2, $is_little) = split(/\s+/);
      $key = sprintf "%6.6d %*s %12.12d",$poolno,$maxfilelen,$file,$offset;
      $keyval{$key} = $offset;
    }

    my $prevpoolno=0;
    my $prevfile="";
    my $fix = 0;
    for $key (sort keys %keyval) {
      my ($poolno,$thisfile,$thisoffset) = split(/\s+/,$key);
      if ($thisfile ne $prevfile) {
	$fix = 0;
	$prevfile = $thisfile;
      }
      $fix -= 20 if ($prevpoolno != $poolno);
      my $offset = $keyval{$key};
      $offset += $fix;
#      print STDERR "$key:$keyval{$key} => $offset ($fix)\n";
      $keyval{$key} = $offset;
      $prevpoolno = $poolno;
    }

    for (@out) {
      my ($col, $colname, $dtname, $dtnum, $file, $poolno, $offset, $length,
	  $pmethod, $pmethod_actual, $nrows, $nmdis, 
	  $avg, $min, $max, $res1, $res2, $is_little) = split(/\s+/);
      $key = sprintf "%6.6d %*s %12.12d",$poolno,$maxfilelen,$file,$offset;
      $offset = $keyval{$key};
      $_ = join(' ',
		($col, $colname, $dtname, $dtnum, $file, $poolno, $offset, $length,
		 $pmethod, $pmethod_actual, $nrows, $nmdis, 
		 $avg, $min, $max, $res1, $res2, $is_little));
    }
  }
}

$dcaline =~ s/^(\#DCA):/${1}2:/;
print "$dcaline\n";
for (@out) {
  print "$_\n";
}


