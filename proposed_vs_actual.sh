#!/bin/bash

IFS=' ' read -r -a array <<< "$@"
Test_CASES=${#array[@]}
#echo "Test_CASES=$Test_CASES"

FILE_NAME="${array[0]}/${array[1]}.compute.proposed_vs_sent_time.out"

LABEL="set xlabel 'contribution number'; set ylabel 'Time in ms'; set xtics 0,10; set ytics 0,1000; set xrange [0:100]; set title"
CMD_F1="$LABEL 'Proposed vs actual sending times of Compute node#${array[1]}'; plot "

CMD_F1="$CMD_F1'../$FILE_NAME' using 3:4 with linespoints title 'Proposed times', "

CMD_F1="$CMD_F1'../$FILE_NAME' using 3:5 with linespoints title 'Actual times' "



echo "CMD_F1=$CMD_F1"
gnuplot -e "$CMD_F1; pause -1"