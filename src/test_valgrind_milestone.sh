#### CS 165 milestone test script           ####
# this script takes generated tests,
# and kicks off running a test for each,
# via another script call for a
# single test case to be run
#
#### Contact: Wilson Qin                    ####
rm -rf cs165.db;
make



UPTOMILE="${1:-5}"

# the number of seconds you need to wait for your server to go from shutdown
# to ready to receive queries from client.
# the default is 5 s.
#
# Note: if you see segfaults when running this test suite for the test cases which expect a fresh server restart,
# this may be because you are not waiting long enough between client tests.
# Once you know how long your server takes, you can cut this time down - and provide it on your cmdline
WAIT_SECONDS_TO_RECOVER_DATA="${2:-5}"

MAX_AVAILABLE_MS=5
MAX_TEST=43
TEST_IDS=`seq -w 1 ${MAX_TEST}`

if [ "$UPTOMILE" -eq "1" ] ;
then
    MAX_TEST=9
elif [ "$UPTOMILE" -eq "2" ] ;
then
    MAX_TEST=17
elif [ "$UPTOMILE" -eq "3" ] ;
then
    MAX_TEST=30
elif [ "$UPTOMILE" -eq "4" ] ;
then
    MAX_TEST=37
elif [ "$UPTOMILE" -eq "5" ] ;
then
    MAX_TEST=43
fi

function killserver () {
    SERVER_NUM_RUNNING=`ps aux | grep server | wc -l`
    if [ $(($SERVER_NUM_RUNNING)) -ne 0 ]; then
        # kill any servers existing
        if pgrep server; then
            pkill -9 server
        fi
    fi
}

FIRST_SERVER_START=0

for TEST_ID in $TEST_IDS
do
    if [ "$TEST_ID" -le "$MAX_TEST" ]
    then
        if [ ${FIRST_SERVER_START} -eq 0 ]
        then
            # start the server before the first case we test.
            ./server > last_server.out &
            FIRST_SERVER_START=1
        elif [ ${TEST_ID} -eq 2 ] || [ ${TEST_ID} -eq 5 ] || [ ${TEST_ID} -eq 11 ] || [ ${TEST_ID} -eq 19 ] || [ ${TEST_ID} -eq 20 ] || [ ${TEST_ID} -eq 29 ] || [ ${TEST_ID} -eq 32 ] || [ ${TEST_ID} -eq 41 ]
        then
            # We restart the server after test 1,4,10,18,19,28,31 (before 2,3,11,12,17,18,29,32), as expected.

            killserver

            # start the one server that should be serving test clients
            # invariant: at this point there should be NO servers running
            valgrind --leak-check=full --track-origins=yes ./server & #1> outputs/server_${TEST_ID}.out 2> outputs/server_${TEST_ID}.err &
            sleep $WAIT_SECONDS_TO_RECOVER_DATA
        fi

        SERVER_NUM_RUNNING=`ps aux | grep server | wc -l`
        if [ $(($SERVER_NUM_RUNNING)) -lt 1 ]; then
            echo "Warning: no server running at this point. Your server may have crashed early."
        fi

        # collect the client output for this test case by test_id
        valgrind --leak-check=full --track-origins=yes ./client < medium/test${TEST_ID}gen.dsl 1> outputs/test${TEST_ID}gen.out 2> outputs/test${TEST_ID}gen.err

        sleep 1

        # run the "comparison" script for comparing against expected output for test_id
        bash verify_output_standalone.sh $TEST_ID outputs/test${TEST_ID}gen.out medium/test${TEST_ID}gen.exp outputs/test${TEST_ID}gen.cleaned.out outputs/test${TEST_ID}gen.cleaned.sorted.out

    fi
done

echo "Milestone Run is Complete up to Milestone #: $UPTOMILE"

killserver
