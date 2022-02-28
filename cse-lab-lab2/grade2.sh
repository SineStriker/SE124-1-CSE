#!/bin/bash
make clean &> /dev/null 2>&1
make &> /dev/null 2>&1 

./stop.sh >/dev/null 2>&1
./stop.sh >/dev/null 2>&1
./stop.sh >/dev/null 2>&1
./stop.sh >/dev/null 2>&1
./stop.sh >/dev/null 2>&1

score=60

mkdir chfs1 >/dev/null 2>&1
mkdir chfs2 >/dev/null 2>&1

./start.sh

test_if_has_mount(){
	mount | grep -q "chfs_client"
	if [ $? -ne 0 ];
	then
			echo "FATAL: Your CHFS client has failed to mount its filesystem!"
			exit
	fi;
	chfs_count=$(ps -e | grep -o "chfs_client" | wc -l)
	extent_count=$(ps -e | grep -o "extent_server" | wc -l)

	if [ $chfs_count -ne 2 ];
	then
			echo "error: chfs_client not found (expecting 2)"
			exit
	fi;

	if [ $extent_count -ne 1 ];
	then
			echo "error: extent_server not found"
			exit
	fi;
}
test_if_has_mount

##################################################

wc_test(){
./test-lab2-part2-a.sh chfs1 chfs2 | grep -q "Passed mr-wc test."
if [ $? -ne 0 ];
then
        echo "Failed test-part2-a"
else
        #exit
		ps -e | grep -q "chfs_client"
		if [ $? -ne 0 ];
		then
				echo "FATAL: chfs_client DIED!"
				exit
		else
			score=$((score+10))
			echo "Passed part2 A (Word Count)"
			#echo $score
		fi
fi
}

wc_test

##################################################

mr_wc_test(){
./test-lab2-part2-b.sh chfs2 | grep -q "Passed mr-wc-distributed test."
if [ $? -ne 0 ];
then
        echo "Failed test-part2-b"
else
        #exit
		ps -e | grep -q "chfs_client"
		if [ $? -ne 0 ];
		then
				echo "FATAL: chfs_client DIED!"
				exit
		else
			score=$((score+30))
			echo "Passed part2 B (Word Count with distributed MapReduce)"
			#echo $score
		fi
fi
}

mr_wc_test

# finally reaches here!
if [ $score -eq 100 ];
then
	echo "Lab2 part 2 passed"
	echo "Passed all tests!"
else
	echo "Lab2 part 2 failed"
fi

./stop.sh

echo ""
echo "Score: "$score"/100"
