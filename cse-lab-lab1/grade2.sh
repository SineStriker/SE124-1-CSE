#!/bin/bash

score=0

mkdir chfs1 >/dev/null 2>&1

./start.sh

test_if_has_mount(){
	mount | grep -q "chfs_client"
	if [ $? -ne 0 ];
	then
			echo "FATAL: Your ChFS client has failed to mount its filesystem!"
			exit
	fi;
}
test_if_has_mount

##################################################################################

./test-lab1-part2-e.sh chfs1 | grep -q "Passed BLOB"
if [ $? -ne 0 ];
then
        echo "Failed test-e"
else
        #exit
		ps -e | grep -q "chfs_client"
		if [ $? -ne 0 ];
		then
				echo "FATAL: chfs_client DIED!"
				exit
		else
			score=$((score+20))
			echo "Passed E"
			#echo $score
		fi
fi

test_if_has_mount

##################################################################################
robust(){
./test-lab1-part2-f.sh chfs1 | grep -q "Passed ROBUSTNESS test"
if [ $? -ne 0 ];
then
        echo "Failed test-f"
else
        #exit
		ps -e | grep -q "chfs_client"
		if [ $? -ne 0 ];
		then
				echo "FATAL: chfs_client DIED!"
				exit
		else
			score=$((score+20))
			echo "Passed F -- Robustness"
			#echo $score
		fi
fi

test_if_has_mount
}

# finally reaches here!
#echo "Passed all tests!"

./stop.sh
echo ""
echo "Part2 score: "$score"/20"
