#!/usr/bin/perl
while (<STDIN>) {
  chomp;
  if (/^([^,]+),(.*?)$/) {
    $key=$1; $rec=$2;
    $key =~ s/\\/&92;/g;
    $rec =~ s/\\/&92;/g;
    print $key, "\n", $rec, "\n";
  }
}
