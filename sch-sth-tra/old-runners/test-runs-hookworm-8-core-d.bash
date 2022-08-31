#!/usr/bin/env bash

# 100 then 50 sims
result_folder=results/$( date +%Y%m%d%H%M%S )
short=Hook
num_sims=200
demogName=KenyaKDHS
mkdir -p $result_folder
IUs="130:ZMB50839 131:COD13882 132:KEN54017 133:MWI28962 134:ZMB50881 135:SWZ45422 136:CMR07495 137:CMR07464 138:CMR07429 139:CMR07606 140:BFA05347 141:NGA36426 142:NGA37015 143:MOZ33156 144:ETH19281 145:UGA49799 146:RWA38112 147:MDG27879 148:TZZ51899 149:ZMB50847 150:TGO48677 151:CMR07442 152:CMR07588 153:NGA36693 154:NGA36635 155:MDG27839 156:UGA49794 157:UGA49727 158:BDI06377 159:ZMB50830 160:CMR07582 161:MDG27877 162:BDI06414 163:BDI06404 164:BDI06408 165:RWA38118 166:RWA38120 167:BDI06399 168:BDI06412 169:BDI06419 170:BDI06375"
scenario=2
for i in $IUs ; do
    iu=$( echo $i | cut -f 2 -d : )
    group=$( echo $i | cut -f 1 -d : )
    CMD="time python3 -u run.py -d $short -g $group -i $iu -s $scenario -n $num_sims -m $demogName"
    echo "*--> running ${CMD}"
    unbuffer bash -c "${CMD}" | tee -a $result_folder/s${scenario}_g${group}_${iu}.out
    echo
done
