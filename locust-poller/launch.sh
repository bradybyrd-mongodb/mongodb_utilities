 #!/bin/bash
echo "#------------------ Performing Load ----------------------#"


script_file="/home/brady_byrd/bbpy/bin/python3 /home/brady_byrd/locust_poller/standalone.py action=load_data"
source /home/brady_byrd/bbpy/bin/activate
arr=("a-1" "a-2" "a-3" "a-4" "a-5" "a-6" "a-7" "a-8" "a-9" "a-10" )
arr=("a-1" "a-2" )
for item in "${arr[@]}"
do
    echo "Proc$item"
    python3 standalone.py action=load_data 2>&1 &
done
