use strict;
use Getopt::Long;

#-- Some env-vars --

my $binpath = $ENV{'ODB_BINPATH'} || ".";
my $febinpath = $ENV{'ODB_FEBINPATH'} || "$binpath";
my $magics_device = $ENV{'MAGICS_DEVICE'} || "JPEG";
my $odb_io_grpsize = $ENV{'ODB_IO_GRPSIZE'} || 0;

#--- Globals ---

my $devnull = "/dev/null";

#--- Argument list processing --

my $ncpus = 1;
my $nchunk_in = 0;
my $npools = 0;
my $dir = "dir.0";

my $Exe = "$febinpath/odbsql.x"; 
my $nccatExe = "/dev/null";

my $schema = $devnull;
my $tmpquery = $devnull;
my $start = 1;
my $maxcount = -1;
my $debug = 0;
my $konvert = 0;
my $format = "default";
my $outfile = $devnull;
my $write_title = 0;
my $global_row_count = 1;
my $b4x = 0;
my $cmap = $devnull;
my $fmt_string = "-";
my $joinstr = 0;

my $view = "myview";
my $errtrg = $devnull;
my $varvalue = "-";

#-- Misc

my $plotobs_exe = "$febinpath/plotobs.x";
my $b4x_exe = "$febinpath/b4.x";

#        perl -w ./odbsqlmp.pl --ncpus $ncpus --nchunk $nchunk --npools $npools --workdir dir.$$ \
#           --executable $Exe --nccat $nccatExe \
#           --schema $schema_file --query $tmpquery \
#           --konvert $konvert --format $format \
#           --outfile $outfile --write_title $write_title \
#           --b4x $b4x --cmap $cmap --fmt_string "$fmt_string" --joinstr $joinstr \
#           --view myview --error error.out \
#           2>$stderr || rc=$?

GetOptions( "ncpus=i" => \$ncpus,
	    "npools=i" => \$npools,
	    "nchunk=i" => \$nchunk_in,
	    "workdir=s" => \$dir,
	    "executable=s" => \$Exe,
	    "nccat=s" => \$nccatExe,
	    "schema_file=s" => \$schema,
	    "query_file=s" => \$tmpquery,
	    "konvert=i" => \$konvert,
	    "format=s" => \$format,
	    "outfile=s" => \$outfile,
	    "write_title=i" => \$write_title,
	    "b4x=i" => \$b4x,
	    "cmap=s" => \$cmap,
	    "fmt_string=s" => \$fmt_string,
	    "joinstr=i" => \$joinstr,
	    "viewname=s" => \$view,
	    "error_trigger=s" => \$errtrg,
	    "varvalue=s" => \$varvalue,
	    ) || die "Invalid option(s)";


my $nchunk = ($nchunk_in < 1) ? ($odb_io_grpsize > 0 ? $odb_io_grpsize : $npools/$ncpus) : $nchunk_in;
$nchunk = $npools if ($nchunk > $npools);

$fmt_string =~ s/%/%%/g;

my $do_cmd_1="%env% $Exe $schema";

my $do_cmd_allpools = $do_cmd_1;

$do_cmd_1        =~ s|%env%|env ODB_PLOTTER=0 ODB_PERMANENT_POOLMASK=%d-%d|;
$do_cmd_allpools =~ s|%env%|env ODB_PLOTTER=0 ODB_PERMANENT_POOLMASK=-1|;

my $do_cmd_2="$start %maxcount% $debug $konvert $format %outfile% %write_title% $global_row_count";
$do_cmd_2 .= " %progress_bar% %b4x% $cmap $fmt_string $joinstr $varvalue";

$do_cmd_2 =~ s|%b4x%|0|;
$do_cmd_2 =~ s|%progress_bar%|0|;
$do_cmd_2 =~ s|%write_title%|%d|;
my $uit = "$dir/uit.%8.8d"; # generic output file
$do_cmd_2 =~ s|%outfile%|$uit|;
$do_cmd_2 =~ s|%maxcount%|%d|;

#-- Start of command file

my $errtouch = "|| :";
if ( $errtrg ne $devnull ) {
    print STDOUT "rm -f $errtrg\n";
    &Wait();
    $errtouch = "|| touch $errtrg";
}

print STDOUT "mkdir $dir\n";
print STDOUT "on_error: rm -rf $dir\n";

&Wait();

my @ayts = ();

my $np = 0;

if ($write_title) { # A hack/trick to produce a title line for sure (as long as at least ONE data row found)
    if ( "$format" eq "odbtk" ||
	 "$format" eq "default" ||
	 "$format" eq "dump" ) {
	my $thismany = ("$format" eq "odbtk") ? 2 : 1;
	printf(STDOUT "$do_cmd_1 $tmpquery \\\n");
	printf(STDOUT "$do_cmd_2 | head -$thismany $errtouch\n", 1, 0, 1); # args: maxcount, start_pool_for_uit, write_title
	#-- Capture the actual output file name used
	my $ayt = sprintf($uit, $np);
	push(@ayts, $ayt);
	printf(STDOUT "[ ! -f $errtrg ] || exit 123\n");
    }
}
$write_title = 0;

my $cnt = 0;
for ($np = 1; $np <= $npools ; $np += $nchunk) {
    $cnt++;
    printf(STDOUT "[ ! -f $errtrg ] || exit 123\n") if ($cnt % $ncpus == 0); # less frequent checking
    my $nlast = $np + $nchunk - 1;
    $nlast = $npools if ($nlast > $npools);
    printf(STDOUT "$do_cmd_1 $tmpquery \\\n", $np, $nlast);
    printf(STDOUT "$do_cmd_2 $errtouch\n", -1, $np, 0); # args: maxcount, start_pool_for_uit, write_title
    #-- Capture the actual output file name used
    my $ayt = sprintf($uit, $np);
    push(@ayts, $ayt);
}
&Wait();
printf(STDOUT "[ ! -f $errtrg ] || exit 123\n");
&Wait();

my $has_gz = ($outfile =~ m/\.gz\b/);

if ( "$format" ne "netcdf" ) {
    if ( $has_gz ) {
	printf(STDOUT "cat $dir/uit.* | gzip -1c > $outfile $errtouch\n","$view");
    }
    else {
	if ( "$outfile" ne "$devnull" ) {
	    printf(STDOUT "cat $dir/uit.* > $outfile $errtouch\n","$view");
	}
	else {
	    printf(STDOUT "cat $dir/uit.* $errtouch\n");
	}
    }
}
elsif ( -x $nccatExe ) { # NetCDF file concatenation
    my $n_ayts = $#ayts + 1; # no. of NetCDF output files
    my $uscores = "";
    while ($n_ayts >= 2) {
	printf(STDOUT "# $n_ayts files to go ...\n");
	$uscores .= "_";
	my $prtcnt = 0;
	my $cnt = 0;
	my $cmd = "";
	my @cmds = ();
	my @new_ayts = ();
	for (@ayts) {
	    $cnt++;
	    if ($cnt % 2 == 1) {
		if ($cnt < $n_ayts) {
		    $cmd = "$nccatExe $_";
		}
		push(@new_ayts, "$_");
	    }
	    else {
		$cmd .= " $_";
		printf(STDOUT "$cmd $errtouch\n");
		$prtcnt++;
		printf(STDOUT "[ ! -f $errtrg ] || exit 123\n") if ($prtcnt % $ncpus == 0); # less frequent checking
	    }
	} # for (@ayts)
	@ayts = @new_ayts;
	$n_ayts = $#ayts + 1;
	&Wait();
    } # while ($n_ayts >= 2)
    printf(STDOUT "# $n_ayts file to go ...\n");
    for (@ayts) {
	if ($has_gz) {
	    printf(STDOUT "gzip -1c < $_ > $outfile $errtouch\n","$view");
	}
	else {
	    printf(STDOUT "mv $_ $outfile $errtouch\n","$view");
	}
	last;
    }
}

&Wait();
printf(STDOUT "[ ! -f $errtrg ] || exit 123\n");
&Wait();

print STDOUT "rm -rf $dir\n";
if ( $b4x == 1 ) {
    if ( "$format" eq "plotobs" ) { # regular plot
	printf(STDOUT "$plotobs_exe -d$magics_device -b$outfile -s &\n","$view");
    }
    elsif ( "$format" eq "wplotobs" ) { # wind arrow plot
	printf(STDOUT "$plotobs_exe -d$magics_device -b$outfile -s -W &\n","$view");
    }
    else {
	printf(STDOUT "$b4x_exe $outfile &\n","$view");
    }
}

#-- End of command file

#--- Subroutines ---

sub Wait {
    print STDOUT "wait\n";
}





