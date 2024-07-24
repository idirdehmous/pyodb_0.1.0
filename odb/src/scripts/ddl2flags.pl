use strict;

#my @in=<DATA>;

my %var=();

#for (@in) {
for (<>) {
    chomp;
    s/^\s*//;
    next if (m/^$/  ||
	     m#^//# ||
	     m/RESET/i);
    if (m/^SET\s+(\S+)\s*=\s*(\d+)\b\s*/i) {
	my $tmp = lc($1);
	$var{$tmp} = $2;
	next;
    }
    s/\s+//g;
    s/^(.*)/\L$1/;
    if (m/^(align|onelooper)\((.*)\)/) {
      my $kind = ($1 eq "align") ? "A" : "1";
      $_ = $2;
      my @v = split(/,/);
      my $first = shift @v;
      print "-$kind$first=";
      my @tmp = ();
      for (@v) {
	if (m/\[/) {
	  if (m/(\w+)\[(\S+):(\S+)\]/) {
	    my $name = $1;
	    my $a = &getval($2);
	    my $b = &getval($3);
	    if ($a <= $b) {
	      for ($a .. $b) {
		my $abs = ($_ >= 0) ? $_ : -$_;
		push(@tmp,($_ >= 0) ? "${name}_$abs" : "${name}__$abs");
	      }
	    }
	  }
	  elsif (m/(\w+)\[(\S+)\]/) {
	    my $name = $1;
	    $_ = &getval($2);
	    my $abs = ($_ >= 0) ? $_ : -$_;
	    push(@tmp,($_ >= 0) ? "${name}_$abs" : "${name}__$abs");
	  }
	}
	else {
	  push(@tmp,$_);
	}
      }
      @v = @tmp;
      my $separ = ($#v == 0) ? "" : "(";
      for (@v) {
	print "$separ$_";
	$separ = ",";
      }
      print ")" if ($#v > 0);
      print "\n";
    }
}

sub getval {
  my ($x) = @_;
  if ($x =~ m/^[-+]?\d+$/) {
    return $x; # is an integer
  }
  elsif (defined($var{$x})) {
    return $var{$x};
  }
  else {
    return -2147483647;
  }
}

__DATA__

//SET $NMXUPD=3;

// Aligned tables (contain the same no. of rows when requested over the @LINK)
RESET ALIGN;
ALIGN(body,errstat,update[1:$NMXUPD],scatt_body,ssmi_body);
ALIGN(atovs,atovs_pred);

// @LINKs with maximum jump of one ("one-loopers")
// Rows in these tables have one-to-one correspondence over the @LINK
RESET ONELOOPER;
ONELOOPER(index,hdr);
ONELOOPER(hdr,sat);
ONELOOPER(sat,atovs,ssmi,scatt[-2:$nmxupd],satob,reo3[$nmxupd]);
