#!/usr/bin/env bash

# 100 then 50 sims
result_folder=results/$( date +%Y%m%d%H%M%S )
short=Hook
num_sims=200
demogName=KenyaKDHS
mkdir -p $result_folder
IUs="1:NGA36997 2:CMR07519 3:SDN53276 4:ETH18986 5:GMB47651 6:TZA46598 7:ETH19304 8:GNB23646 9:ETH18864 10:MDG27943 11:MRT31039 12:RWA38129 13:AGO02177 14:KEN54081 15:SOM56002 16:COD14016 17:GMB47641 18:GHA21393 19:COD14017 20:TZA46546 21:ETH18531 22:GNB23740 23:ERI17440 24:CIV15329 25:RWA38137 26:UGA49756 27:ERI17446 28:ZWE52929 29:UGA49749 30:SWZ45432 31:UGA49710 32:CMR07424 33:MOZ33254 34:ETH19257 35:NGA36403 36:ETH19322 37:TZA46527 38:GHA21411 39:MOZ33178 40:SSD44361 41:TZA46641 42:SSD44354 43:MDG27923"
scenario=2
for i in $IUs ; do
    iu=$( echo $i | cut -f 2 -d : )
    group=$( echo $i | cut -f 1 -d : )
    CMD="time python3 -u run.py -d $short -g $group -i $iu -s $scenario -n $num_sims -m $demogName"
    echo "*--> running ${CMD}"
    unbuffer bash -c "${CMD}" | tee -a $result_folder/s${scenario}_g${group}_${iu}.out
    echo
done
