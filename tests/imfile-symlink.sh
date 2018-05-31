#!/usr/bin/bash

output="/var/log/messages"
rsyslogconf="/etc/rsyslog.conf"
rsyslogconf_bak=`mktemp`
cp $rsyslogconf $rsyslogconf_bak
watchdir0="/var/log/test0"
targetdir0="/var/log/target0"
reltarget0="../target0"
watchdir1="/var/log/test1"
targetdir1="/var/log/target1"
reltarget1="../target1"

if [ "$EUID" -ne 0 ]; then
  echo "This script $0 needs root permission."
  exit 77 # no root, skip
fi

CWD=`pwd`
if [ "$CWD" == "$watchdir0" -o "$CWD" == "$watchdir1" ]; then
  echo "This script $0 should not be launched in $watchdir0 or $watchdir1."
  exit
fi

# Log count per test case.
MAX=1000

add_logs() {
  local logfile=$1
  local caseid=$2
  local message=$3

  count=0
  echo `date +"%b %d %H:%M:%S"` `hostname -s` test_tag: mytest[720]: [system] test start $logfile > $logfile
  while [ $count -lt $MAX ]; do
    echo `date +%b' '%d' '%H:%M:%S` `hostname -s` test_tag: mytest[720]: [system] test case $caseid $message `date +%d_%N` >> $logfile
    count=`expr $count + 1`
  done
}

restart_rsyslogd() {
  local setup_test_config=${1:-}
  systemctl stop rsyslog.service

  if [ -n "$setup_test_config" ]; then
    output_bak=`mktemp`
    mv $output $output_bak
    touch $output

    sed -i -e 's/\(^module.load="im.*\)/#\1/' $rsyslogconf
    sed -i -e "s/\$ModLoad im.*/# &/" $rsyslogconf

    sed -i '/#### MODULES ####/ a\
module(load="imfile" Mode="inotify")\
\
input(type="imfile" File="/var/log/test0/file*.log"\
Tag="tag0"\
Severity="info"\
Facility="local7")\
Statefile="/var/spool/rsyslog/statefile0"\
\
input(type="imfile" File="/var/log/test1/file*.log"\
Tag="tag1")\
Statefile="/var/spool/rsyslog/statefile1"' $rsyslogconf

    sed -i '/#### GLOBAL DIRECTIVES ####/ a\
# Disable rate-limiting of log entries\
$SystemLogRateLimitInterval 0\
$SystemLogRateLimitBurst 0' $rsyslogconf

    diff -twU4 $rsyslogconf_bak $rsyslogconf
  fi

  rm -rf $watchdir0 $watchdir1 $targetdir0 $targetdir1 || :
  mkdir -p $watchdir0 $watchdir1 $targetdir0 $targetdir1 || :

  systemctl start rsyslog.service
  systemctl status -l rsyslog.service
}

cleanup() {
  systemctl stop rsyslog.service
  cp $rsyslogconf_bak $rsyslogconf
  systemctl start rsyslog.service
  systemctl status -l rsyslog.service
}

symlink_test() {
  caseid=$1
  title=$2
  logfile=$3
  configure=$4
  samedir=$5
  isrelative=$6

  subcase1=${7:-""}
  subcase2=${8:-""}
  subcase3=${9:-""}

  echo "###"
  echo "### TEST CASE $caseid -- $title"
  echo "###"

  restart_rsyslogd $configure

  mytargetdir=$watchdir0
  if [ -n "$samedir" -a -n "$isrelative" -a "$samedir" == "false" ]; then
    mytargetdir=$targetdir0
  fi
  if [ -n "$subcase1" ]; then
    add_logs $mytargetdir/$logfile $caseid "message"
  
    if [ -n "$samedir" -a -n "$isrelative" ]; then
      if [ "$samedir" == "true" ]; then
        if [ "$isrelative" == "true" ]; then
          (cd $watchdir0; ln -s $logfile $logfile.log)
        else
          ln -s $watchdir0/$logfile $watchdir0/$logfile.log
        fi
      else
        if [ "$isrelative" == "true" ]; then
          (cd $watchdir0; ln -s $reltarget0/$logfile $logfile.log)
        else
          ln -s $targetdir0/$logfile $watchdir0/$logfile.log
        fi
      fi
      ls -l $watchdir0/$logfile.log
      ls -lL $watchdir0/$logfile.log
    fi 
  
    sleep 1
  
    rc=$( egrep "test case $caseid message" $output | wc -l )
  
    echo $subcase1
    if [ $rc -ne $MAX ]; then
      echo $title ": expected result: $MAX, actual: $rc -- failed"
    else
      echo $title ": expected result: $MAX, actual: $rc -- passed"
    fi
  fi

  if [ -n "$subcase2" ]; then
    mv $mytargetdir/$logfile $mytargetdir/$logfile.bak
  
    add_logs $mytargetdir/$logfile "$caseid-1" "message"
  
    sleep 1
  
    rc=$( egrep "test case $caseid-1 message" $output | wc -l )
  
    echo $subcase2
    if [ $rc -ne $MAX ]; then
      echo $title ": expected result: $MAX, actual: $rc -- failed"
    else
      echo $title ": expected result: $MAX, actual: $rc -- passed"
    fi
  fi

  if [ -n "$subcase3" ]; then
    rm -rf $mytargetdir/$logfile
  
    add_logs $mytargetdir/$logfile "$caseid-2" "message"
  
    sleep 1
  
    rc=$( egrep "test case $caseid-2 message" $output | wc -l )
  
    echo $subcase3
    if [ $rc -ne $MAX ]; then
      echo $title ": expected result: $MAX, actual: $rc -- failed"
    else
      echo $title ": expected result: $MAX, actual: $rc -- passed"
    fi
  fi
}

symlink_test_parallel() {
  caseid=$1
  title=$2
  logfile=$3
  configure=$4
  samedir=$5
  isrelative=$6

  subcase1=${7:-""}
  subcase2=${8:-""}
  subcase3=${9:-""}

  echo "###"
  echo "### TEST CASE $caseid -- $title"
  echo "###"

  restart_rsyslogd $configure

  mytargetdirs=($watchdir0 $watchdir1)
  if [ -n "$samedir" -a -n "$isrelative" -a "$samedir" == "false" ]; then
    mytargetdirs=($targetdir0 $targetdir1)
  fi
  if [ -n "$subcase1" ]; then
    add_logs ${mytargetdirs[0]}/$logfile "$caseid-0" "parallel message" &
    add_logs ${mytargetdirs[1]}/$logfile "$caseid-1" "parallel message"
  
    if [ -n "$samedir" -a -n "$isrelative" ]; then
      if [ "$samedir" == "true" ]; then
        if [ "$isrelative" == "true" ]; then
          (cd $watchdir0; ln -s $logfile $logfile.log)
          (cd $watchdir1; ln -s $logfile $logfile.log)
        else
          ln -s $watchdir0/$logfile $watchdir0/$logfile.log
          ln -s $watchdir1/$logfile $watchdir1/$logfile.log
        fi
      else
        if [ "$isrelative" == "true" ]; then
          (cd $watchdir0; ln -s $reltarget0/$logfile $logfile.log)
          (cd $watchdir1; ln -s $reltarget1/$logfile $logfile.log)
        else
          ln -s $targetdir0/$logfile $watchdir0/$logfile.log
          ln -s $targetdir1/$logfile $watchdir1/$logfile.log
        fi
      fi
      ls -l $watchdir0/$logfile.log
      ls -lL $watchdir0/$logfile.log
      ls -l $watchdir1/$logfile.log
      ls -lL $watchdir1/$logfile.log
    fi 
  
    sleep 1
  
    rc0=$( egrep "test case $caseid-0 parallel message" $output | wc -l )
    rc1=$( egrep "test case $caseid-1 parallel message" $output | wc -l )
  
    echo $subcase1
    if [ $rc0 -ne $MAX -o $rc1 -ne $MAX ]; then
      echo $title ": expected result: $MAX, actual: $rc0, $rc1 -- failed"
    else
      echo $title ": expected result: $MAX, actual: $rc0, $rc1 -- passed"
    fi
  fi

  if [ -n "$subcase2" ]; then
    mv ${mytargetdirs[0]}/$logfile ${mytargetdirs[0]}/$logfile.bak
    mv ${mytargetdirs[1]}/$logfile ${mytargetdirs[1]}/$logfile.bak
  
    add_logs ${mytargetdirs[0]}/$logfile "$caseid-0-1" "parallel message" &
    add_logs ${mytargetdirs[1]}/$logfile "$caseid-1-1" "parallel message"
  
    sleep 1
  
    rc0=$( egrep "test case $caseid-0-1 parallel message" $output | wc -l )
    rc1=$( egrep "test case $caseid-1-1 parallel message" $output | wc -l )
  
    echo $subcase2
    if [ $rc0 -ne $MAX -o $rc1 -ne $MAX ]; then
      echo $title ": expected result: $MAX, actual: $rc0, $rc1 -- failed"
    else
      echo $title ": expected result: $MAX, actual: $rc0, $rc1 -- passed"
    fi
  fi

  if [ -n "$subcase3" ]; then
    rm -rf ${mytargetdirs[0]}/$logfile
    rm -rf ${mytargetdirs[1]}/$logfile
  
    add_logs ${mytargetdirs[0]}/$logfile "$caseid-0-2" "parallel message" &
    add_logs ${mytargetdirs[1]}/$logfile "$caseid-1-2" "parallel message"
  
    sleep 1
  
    rc0=$( egrep "test case $caseid-0-2 parallel message" $output | wc -l )
    rc1=$( egrep "test case $caseid-1-2 parallel message" $output | wc -l )
  
    echo $subcase3
    if [ $rc0 -ne $MAX -o $rc1 -ne $MAX ]; then
      echo $title ": expected result: $MAX, actual: $rc0, $rc1 -- failed"
    else
      echo $title ": expected result: $MAX, actual: $rc0, $rc1 -- passed"
    fi
  fi
}

symlink_test 1 "ordinary input file" \
"file_test1.log" "true" "" "" \
"0. create a file; add logs to the file." "" ""

symlink_test 2 "symbolic link; target with relative path is in the same watched log dir" \
"file_test2" "" "true" "true" \
"0. create a target; add logs to the target; make a symbolic link pointing to the target." \
"1. rename target; add logs to the new target; check the logs are in the rsyslog output." \
"2. remove target; add logs to the new target; check the logs are in the rsyslog output."

symlink_test 3 "symbolic link; target with absolute path is in the same watched log dir" \
"file_test3" "" "true" "false" \
"0. create a target; add logs to the target; make a symbolic link pointing to the target." \
"1. rename target; add logs to the new target; check the logs are in the rsyslog output." \
"2. remove target; add logs to the new target; check the logs are in the rsyslog output."

symlink_test 4 "symbolic link; target with relative path is in a non-watched dir" \
"file_test4" "" "false" "true" \
"0. create a target; add logs to the target; make a symbolic link pointing to the target." \
"1. rename target; add logs to the new target; check the logs are in the rsyslog output." \
"2. remove target; add logs to the new target; check the logs are in the rsyslog output."

symlink_test 5 "symbolic link; target with absolute path is in the same watched log dir" \
"file_test5" "" "false" "false" \
"0. create a target; add logs to the target; make a symbolic link pointing to the target." \
"1. rename target; add logs to the new target; check the logs are in the rsyslog output." \
"2. remove target; add logs to the new target; check the logs are in the rsyslog output."

symlink_test_parallel 6 "ordinary input files" \
"file_test6.log" "" "" "" \
"0. create 2 files; add logs to the files." "" ""

symlink_test_parallel 7 "2 input files; symbolic link; target with relative path is in the same watched log dir" \
"file_test7" "" "true" "true" \
"0. create a target; add logs to the target; make a symbolic link pointing to the target." \
"1. rename target; add logs to the new target; check the logs are in the rsyslog output." \
"2. remove target; add logs to the new target; check the logs are in the rsyslog output."

symlink_test_parallel 8 "2 input files; symbolic link; target with absolute path is in the same watched log dir" \
"file_test8" "" "true" "false" \
"0. create a target; add logs to the target; make a symbolic link pointing to the target." \
"1. rename target; add logs to the new target; check the logs are in the rsyslog output." \
"2. remove target; add logs to the new target; check the logs are in the rsyslog output."

symlink_test_parallel 9 "2 input files; symbolic link; target with relative path is in a non-watched dir" \
"file_test9" "" "false" "true" \
"0. create a target; add logs to the target; make a symbolic link pointing to the target." \
"1. rename target; add logs to the new target; check the logs are in the rsyslog output." \
"2. remove target; add logs to the new target; check the logs are in the rsyslog output."

symlink_test_parallel 10 "2 input files; symbolic link; target with absolute path is in the same watched log dir" \
"file_test10" "" "false" "false" \
"0. create a target; add logs to the target; make a symbolic link pointing to the target." \
"1. rename target; add logs to the new target; check the logs are in the rsyslog output." \
"2. remove target; add logs to the new target; check the logs are in the rsyslog output."

cleanup
