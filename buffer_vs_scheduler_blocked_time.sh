#!/bin/bash

IFS=' ' read -r -a array <<< "$@"
Test_CASES=${#array[@]}
#echo "Test_CASES=$Test_CASES"

FILE_NAME="${array[0]}/${array[1]}.compute.proposed_vs_sent_time.out"
FILE_NAME2="${array[0]}/${array[1]}compute.scheduler_send_time.out"
FILE_NAME3=".blocked_times.out"

$(rm "../${array[0]}/ALL$FILE_NAME3")
$(cat ../${array[0]}/*$FILE_NAME3 > ../${array[0]}/ALL$FILE_NAME3)

INPUT_COUNT=$(awk -v max=0 'FNR > 1 {if($1>=max){max=$1+1}}END{print max} ' ../$FILE_NAME)
START_TS=$(awk -v min=10000 'FNR > 1 {if($2<min){min=$2}}END{print min} ' ../$FILE_NAME)

LABEL="set xlabel 'contribution number'; set ylabel 'Time in ms'; set xtics $START_TS,$INPUT_COUNT; set ytics 0,10; set xrange [$START_TS:1000]; set title"
CMD_F1="$LABEL 'Proposed vs actual sending times of Compute node#${array[1]}'; plot "

CMD_F1="$CMD_F1'../$FILE_NAME' using 3:4 with linespoints title 'Proposed times', "

CMD_F1="$CMD_F1'../$FILE_NAME' using 3:5 with linespoints title 'Actual times', "

CMD_F1="$CMD_F1'../$FILE_NAME2' using 3:4 with points title 'Interval send times', "

#CMD_F1="$CMD_F1'../${array[0]}/ALL$FILE_NAME3' every ::1::1000 using 3:(sum = $4 + $5, sum/2):4:5 with xerrorbars title 'Scheduler blocked time', "
    
CMD_F1="$CMD_F1'../${array[0]}/ALL$FILE_NAME3' every ::1::1000 using 3:1:6:7 with errorbars title 'Buffer blocked time' "

echo "CMD_F1=$CMD_F1"
gnuplot -e "$CMD_F1; pause -1"