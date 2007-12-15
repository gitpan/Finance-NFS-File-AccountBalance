package Finance::NFS::File::AccountBalance;

use Moose;
use IO::File;
use DateTime;

our $VERSION = '0.001000';

my $rh  = qr/^H(.{3}).{17}(.{16}).{4}(\d{6}).{4}(.{15}).{35}\r?$/;
my $rt  = qr/^T.{20}(.{15}).{5}(.{15}).{45}\r?$/;
my $r01 = qr/^01(.{3})(.{6})(.{6})(.)(.{13})(.)(.{13})(.)(.{13})(.)(.{13})(.)(.{13})(.)(.{13})\r?$/;
my $r02 = qr/^02(.)(.{13})(.)(.{13})(.)(.{13})(.)(.{13})(.)(.{13})(.)(.{13})(.)(.{13}).\r?$/;
my $r03 = qr/^03(.)(.{13})(.)(.{13})(.)(.{13})(.)(.{13})(.)(.{13})(.)(.{13})(.)(.{13}).\r?$/;
my $r04 = qr/^04(.{5})(.)(.{13})(.)(.{13})(.)(.{13})(.)(.{13})(.)(.{13})(.)(.)(.)(.).(.)(.)(.)(.)(.)(.)(.)(.{9})(.)..\r?$/;
my $r05 = qr/^05(.{10})(.{2})(.{3})(.{3})(.{8})(.{2})(.{4})(.{3}).{3}(.)(.{13})(.)(.{13})(.)(.{13}).{19}\r?$/;
my $rtl = qr/^(?:0[6-9]|10)(.)(.)(.{13})(.)(.{13})(.)(.{13})(.)(.)(.{13})(.)(.{13})(.)(.{13}).{13}\r?$/;

sub trim{
  my $x = shift;
  $x =~ s/^\s+//;
  $x =~ s/\s+$//;
  return $x;
}

has filename         => (is => 'ro', isa => 'Str', required => 1);
has client_id        => (is => 'ro', isa => 'Str', required => 1);
has account_callback => (is => 'ro', isa => 'CodeRef', predicate => 'has_account_callback');

has _file_handle => (is => 'ro', isa => 'IO::File', lazy_build => 1);
has records      => (is => 'ro', isa => 'ArrayRef', lazy_build => 1);
has file_date    => (
                     isa       => 'DateTime',
                     reader    => 'file_date',
                     writer    => '_file_date',
                     predicate => 'has_file_date',
                    );

sub _build__file_handle {
  my $self = shift;
  my $file = $self->filename;
  confess("file $file is not readable by effective uid/gid")
    unless -r $file;
  IO::File->new("<${file}") or confess("Failed to open $file");
}

sub BUILD{
  my $self = shift;
  $self->_process_header;
}

sub clean_date {
  my ($self, $date) = @_;
  if (my($month,$day,$year) = ($date =~ /^(\d\d)(\d\d)(\d\d)$/)) {
    my $now = DateTime->now;
    if ( ($now->year + 1 == 2000 + $year) && $month eq '01' && $day eq '01') {
      $year = 2000 + $year;
    } elsif ( $now->year < 2000 + $year ) {
      $year = 1900 + $year;
    } else {
      $year = 2000 + $year;
    }
    return DateTime->new(year => $year, month =>  $month, day => $day);
  }
  confess "regex failed on $date";
  return;
}

sub _process_header {
  my $self= shift;
  $self->clear_file_handle if $self->_has_file_handle;

  my $io = $self->_file_handle;
  defined(my $line = $io->getline) or confess("file is empty.");

  if ($line =~ /$rh/) {
    confess("client_id " . $self->client_id . " and file client id $1 do not match")
      unless $self->client_id eq $1;
    $self->_file_date( $self->clean_date($3) );
  } else {
    confess("Expected Header but got: '$line'");
  }
}

sub _build_records {
  my $self= shift;

  my $record_count = 1; #header is already processed
  my $io = $self->_file_handle;
  defined(my $line = $io->getline) or confess "File ended without first record";
  my $accounts = [];
  while ( defined($line) ) {
    if ( $line =~ /$r01/) {
      $record_count++;
      my $acct = {
                  branch           => $1,
                  acct_num         => $2,
                  last_update_d    => $3,
                  networth         => $4.$5   / 100,
                  cash_collected   => $6.$7   / 100,
                  collected        => $8.$9   / 100,
                  net_trade_date   => $10.$11 / 100,
                  networth_mkt_val => $12.$13 / 100,
                  cash_money_mkts  => $14.$15 / 100,
                 };

      $acct->{last_update_d} = $self->clean_date($3);
      push(@$accounts, $acct);

      defined($line = $io->getline) or
        confess("File ended prematurely on 01 record at line $.");
      if ($line =~ /$r02/) {
        $record_count++;
        $acct->{option_mkt_val}         = $1.$2   / 100;
        $acct->{option_in_money}        = $3.$4   / 100;
        $acct->{memo_adjustments}       = $5.$6   / 100;
        $acct->{margin_available}       = $7.$8   / 100;
        $acct->{corp_bond_buying_power} = $9.$10  / 100;
        $acct->{muni_bond_buying_power} = $11.$12 / 100;
        $acct->{govt_bond_buying_power} = $13.$14 / 100;

        defined($line = $io->getline) or
          confess("File ended prematurely on 02 record at line $.");
        if ($line =~ /$r03/) {
          $record_count++;
          $acct->{house_surplus_call}   = $1.$2   / 100;
          $acct->{nyse_surplus_call}    = $3.$4   / 100;
          $acct->{sma_fed_call}         = $5.$6   / 100;
          $acct->{min_equity_call}      = $7.$8   / 100;
          $acct->{core_money_mkt}       = $9.$10  / 100;
          $acct->{margin_equity}        = $11.$12 / 100;
          $acct->{margin_liquid_equity} = $13.$14 / 100;

          defined($line = $io->getline) or
            confess("File ended prematurely on 03 record at line $.");
          if ($line=~ /$r04/) {
            $record_count++;
            my $r04 = qr/^04(.{5})(.)(.{13})(.)(.{13})(.)(.{13})(.)(.{13})(.)(.{13})(.)(.)(.)(.).(.)(.)(.)(.)(.)(.)(.)(.{9})(.)..\r?$/;
            $acct->{margin_equity_pct}            = $1      / 100;
            $acct->{fed_call_reduction}           = $2.$3   / 100;
            $acct->{house_call_reduction}         = $4.$5   / 100;
            $acct->{nyse_call_reduction}          = $6.$7   / 100;
            $acct->{uncollected}                  = $8.$9   / 100;
            $acct->{min_equity_call_reduction}    = $10.$11 / 100;
            if(length(my $tmp = trim $12)) { $acct->{transfer_legend_code} = $tmp; }
            $acct->{margin_papers_switch}         = $13;
            $acct->{position_switch}              = $14;
            $acct->{unpriced_switch}              = $15;
            $acct->{acct_type_switch}             = $16;
            $acct->{short_pos_switch}             = $17;
            $acct->{long_pos_switch}              = $18;
            $acct->{memo_entries_switch}          = $19;
            $acct->{day_trades_switch}            = $20;
            $acct->{possible_liquidations_switch} = $21;
            $acct->{min_fed_call_switch}          = $22;
            $acct->{irs_number}                   = $23;
            $acct->{irs_code}                     = $24;

            defined($line = $io->getline) or
              confess("File ended prematurely on 04 record at line $.");
            if ($line=~ /$r05/) {
              $record_count++;
              $acct->{short_name}             = trim $1; #trim
              $acct->{acct_class}             = $2;
              $acct->{owning_rr}              = $3;
              $acct->{exec_rr}                = $4;
              $acct->{agency_code}            = $5;
              if(length(my $tmp = trim $6)) { $acct->{prod_level} = $tmp; }
              $acct->{reg_type}               = trim $7;
              my $type_level_cnt              = $8 * 1;
              $acct->{available_cash}         = $9.$10  / 100;
              $acct->{available_cash_margin}  = $11.$12 / 100;
              $acct->{available_non_margin}   = $13.$14 / 100;
              my $types = $acct->{type_level} = {};

              defined($line = $io->getline) or
                confess("Possibly truncated file ended on 05 records at line $.");
              while ($line =~ /$rtl/ ) { # records 06-10
                $record_count++;
                confess("More type level records than expected at line: $. ")
                  unless $type_level_cnt;
                $types->{$1} = {
                                mkt_value  => $2.$3 / 100,
                                td_balance => $4.$5 / 100,
                                sd_balance => $6.$7 / 100,
                               };
                $type_level_cnt--;
                if ( $type_level_cnt ) {
                  $types->{$8} = {
                                  mkt_value  => $9.$10  / 100,
                                  td_balance => $11.$12 / 100,
                                  sd_balance => $13.$14 / 100,
                                 };
                  $type_level_cnt--;
                }
                #because we still need a header...
                defined($line = $io->getline) or
                  confess("Possibly truncated file ended on 06-10 records at line $.");
              }
              #make sure the number of typelevel records matches how many we got
              confess("Less type level records ( $type_level_cnt ) than expected at line: $. ") if $type_level_cnt;
              #optionally execute the callback on accounts for better integration into
              #async workflows. this could probably use some improvement
              $self->account_callback->($self, $acct) #pass a copy of the instance
                if $self->has_account_callback;

            } else { confess "Got '$line' where 05 record was expected."; }
          } else { confess "Got '$line' where 04 record was expected."; }
        } else { confess "Got '$line' where 03 record was expected."; }
      } else { confess "Got '$line' where 02 record was expected."; }
    } elsif ( $line =~ /$rt/) {
      $record_count++;
      my $record_target_count  = $1;
      my $account_target_count = $2;

      #check recount count match
      confess("Record count ($record_count) and target record count ($record_target_count) mismatched ")
        if $record_target_count != $record_count;
      #check account count match
      confess("Account count (".scalar(@$accounts).") and target account count ($account_target_count) mismatched ")
        if $account_target_count != scalar(@$accounts);
      $self->_clear_file_handle; #close the filehandle

      return $accounts;
    } else {
      confess("Unable to process line $line ($.)");
    }
  }
  confess("Recieved no Trail record. File possibly truncated. line: $. ");
}

__PACKAGE__->meta->make_immutable;

1;

__END__;

=head1 NAME

Finance::NFS::File::AccountBalance - Read Account Balance Records into data structures

=head1 SYNOPSYS

    my $balance = Finance::NFS::File::AccountBalance
      ->new(
            client_id        => 'A7T',
            filename         => $path_to_file,
            account_callback => sub{ ... }, #optional
           );
    my $fbsi_cycle_date = $balance->file_date;
    my $records = eval { $balance->records };
    die("There was an error reading the file: $@") unless defined $records;

    for my $acct ( @{  }){
       #insert records into database or whatever
       my $branch = $acct->{branch}; #etc..
    }

=head1 A note about Finance::NFS::File::*

This family of modules is intended to help developers in the financial industry deal with
the standard transmission files provided by National Financial Services (NFS)

This module focuses on the Account Balance Transmission File.

The file layout version this moduled was designed with in mind is 4.6.4

=head1 ACCOUNT KEYS

Filler fields are discarded; where appropriate, fields are trimed of whitespace; where
applicable, balance figures are merged with their signs and divided by a hundred to
indicate full dollar amounts. Dates is the MMDDYY format are converted to DateTime objects.

The keys available in the account hashref follow, ordered by record of origin and order
whithin that record.

=head2 Record 01

=over

=item * branch

=item * acct_num

=item * last_update_d

=item * networth

=item * cash_collected

=item * collected

=item * net_trade_date

=item * networth_mkt_val

=item * cash_money_mkts

=back

=head2 Record 02

=over

=item * option_mkt_val

=item * option_in_money

=item * memo_adjustments

=item * margin_available

=item * corp_bond_buying_power

=item * muni_bond_buying_power

=item * govt_bond_buying_power

=back

=head2 Record 03

=over

=item * house_surplus_call

=item * nyse_surplus_call

=item * sma_fed_call

=item * min_equity_call

=item * core_money_mkt

=item * margin_equity

=item * margin_liquid_equity

=back

=head2 Record 04

=over

=item * margin_equity_pct

=item * fed_call_reduction

=item * house_call_reduction

=item * nyse_call_reduction

=item * uncollected

=item * min_equity_call_reduction

=item * transfer_legend_code

=item * margin_papers_switch

=item * position_switch

=item * unpriced_switch

=item * acct_type_switch

=item * short_pos_switch

=item * long_pos_switch

=item * memo_entries_switch

=item * day_trades_switch

=item * possible_liquidations_switch

=item * min_fed_call_switch

=item * irs_number

=item * irs_code

=back

=head2 Record 05

=over

=item * short_name

=item * acct_class

=item * owning_rr

=item * exec_rr

=item * agency_code

=item * prod_level

=item * reg_type

=item * available_cash

=item * available_cash_margin

=item * available_non_margin

=back

=head2 Record 06-10

Account Records 06-10 contain the type level balance information and are stored in the
the C<type_level> key as a hashref where the key is the account type (0-9) and the value
is a hashref with the following keys:

=over

=item * mkt_value

=item * td_balance

=item * sd_balance

=back

=head1 ATTRIBUTES

=head2 client_id

Required read-only string value of a 3 character length, usually your Super Branch ID.

=head2 filename

Required read-only string that represents the path to the file you wish to read.

=head2 account_callback

=over 4

=item B<has_account_callback> - predicate

=back

Optional read-only code reference that will be called after the last record of
every account. Will be passed two arguments, $the current instance of the file
parser (so you can access file properties) and a hashref representing the account
as described above.

You can use this to reduce the memory foot print of your program by keeping only
the current account record in memory. Example:

    my $callback = sub{
        my($instance, $acct) = @_;
        #process data here;
        %$acct = ();
    };
    my $balance = Finance::NFS::File::AccountBalance
      ->new(client_id => 'A7T', filename => $file, account_callback => $callback);

    #by the time this returns a callback will have been executed for each account
    my $records = $balance->records;

The downside of this method is that if the file is currupted later on, you will have
to catch the exception and rollback manually. Partially transmitted files are NOT that
uncommon! Make sure you have a rollback mechanism.

=head2 records

=over 4

=item B<clear_records> - clearer

=item B<has_records> - predicate

=item B<_build_records> - builder

=back

An array reference containing all of the accounts in the structure described above.
This read-only attribute builds lazyly the first time it is requested by actually
going through the while file and reading it. If any errors are encountered while
reading the file or the file appears to be truncated an exception will be thrown.

=head2 file_date

=over 4

=item B<_file_date> - set accessor

=item B<has_file_date> - predicate

=back

The date of the FBSI cycle is contained in the header record. When the header record is
processed this attribute is automatically set. This happens when you first instantiate
the object.

=head2 _file_handle

=over 4

=item B<_clear_file_handle> - clearer

=item B<_has_file_handle> - predicate

=item B<_build__file_handle> - builder

=back

This is the IO::File object that holds our filehandle. DO NOT TOUCH THIS. If you mess
with this I can alsmost guarantee you will break something.

=head1 METHODS

=head2 new

Creates a new instance. Takes a list of key / value pairs as arguments. The keys
accepted are the attributes listed above.

=head2 BUILD

At instantiation time this method is called and it opens the file handle and reads
the header of the file to get the file date.

See L<Moose> for more information about how C<BUILD> works.

=head2 _process_header

This private method reads the first line of the file and processes the header.
It also sets the file_date attribute.

=head2 check_file

Takes one argument, a file name, and opens and checks the record structure to make
sure it is correctly formed. Returns 1 if the file is correctly formed and a throw
an exception if it is not.

=head2 clean_date

This will convert a MMDDYY date to a DateTime object. In the future this method
may move to an external module so it can be shared amongst the many file processors
I plan on eventually writting.

=head2 meta

See L<Moose>

=head1 TODO

=over

=item Tests are not yet in place, but I use this module in production. As soon as I have time
to create test data files with bogus data I will include better test coverage.

=item I am considering replacing the confess calls with an error variable and
C<return;> to make it less painful to deal with malformed files, which is not unheard
of, but it does happen sometimes. on the otherhand, I do like throwing exceptions,
it works well for me.

=head1 KNOWN ISSUES

The caveat of converting dates is that if you process a 100 yeard old January 1st
file on December 31st while you are on a timezone behind Fidelity's, such that their
cycle year is yours + 1, the processor will think the date file is actually today's
file. This is likely to never happen in the real world. I really hope nobody is using
this program on FBSI's 100 year anniversary to process 100 year old files.

Dates before the UNIX epoch are not guaranteed to work, but I think that's OK.

=head1 AUTHOR

Guillermo Roditi (groditi) <groditi@cpan.org>

Your name could be here (please contribute!)

=head1 BUGS, FEATURE REQUESTS AND CONTRIBUTIONS

Google Code Project Page - L<http://code.google.com/p/finance-nfs-file-accountbalance/>


=head1 LICENSE

You may distribute this code under the same terms as Perl itself.

=cut

