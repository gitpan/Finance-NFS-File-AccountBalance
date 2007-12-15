#! /usr/bin/perl -w

# Load the Module::Install bundled in ./inc/
use inc::Module::Install;

# Define metadata
name 'Finance-NFS-File-AccountBalance';
abstract "Read National Financial Service's Account Balance file into data structures";
all_from 'lib/Finance/NFS/File/AccountBalance.pm';

# Specific dependencies
requires 'DateTime';
requires 'Moose' => '0.28';
requires 'IO::File';

auto_install;
WriteAll;


