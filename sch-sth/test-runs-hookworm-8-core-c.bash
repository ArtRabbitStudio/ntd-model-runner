#!/usr/bin/env bash

# 100 then 50 sims
result_folder=results/$( date +%Y%m%d%H%M%S )
short=Hook
num_sims=200
demogName=KenyaKDHS
mkdir -p $result_folder
IUs="87:MDG27900 88:COD13880 89:TZZ51901 90:UGA49767 91:MDG27922 92:ZWE52940 93:KEN54276 94:MWI28948 95:MWI28950 96:KEN54227 97:SWZ45405 98:SWZ45411 99:CMR07482 100:CMR07530 101:CMR07422 102:CMR07514 103:CMR07475 104:CMR07595 105:GHA21526 106:ETH19140 107:ETH18957 108:NGA36962 109:MOZ33134 110:ETH19034 111:MOZ33203 112:GNB23719 113:MLI29998 114:ETH18954 115:ETH18818 116:NGA36833 117:ETH19122 118:ETH18936 119:NGA36789 120:UGA49755 121:UGA49768 122:UGA49743 123:MDG27856 124:UGA49762 125:UGA49782 126:RWA38117 127:UGA49729 128:UGA49744 129:ZMB53189"
scenario=2
for i in $IUs ; do
    iu=$( echo $i | cut -f 2 -d : )
    group=$( echo $i | cut -f 1 -d : )
    CMD="time python3 -u run.py -d $short -g $group -i $iu -s $scenario -n $num_sims -m $demogName"
    echo "*--> running ${CMD}"
    unbuffer bash -c "${CMD}" | tee -a $result_folder/s${scenario}_g${group}_${iu}.out
    echo
done
