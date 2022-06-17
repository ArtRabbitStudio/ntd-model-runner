#!/usr/bin/env bash

# 100 then 50 sims
result_folder=results/$( date +%Y%m%d%H%M%S )
short=Hook
num_sims=200
demogName=KenyaKDHS
mkdir -p $result_folder
IUs="44:RWA38125 45:UGA49765 46:RWA38138 47:UGA49732 48:TZZ51896 49:MWI28953 50:UGA49805 51:UGA49758 52:KEN54212 53:UGA49707 54:SWZ45425 55:CMR07439 56:CMR07427 57:CMR07525 58:ZMB50883 59:MDG27866 60:GHA21399 61:ETH18555 62:GHA21512 63:ETH18656 64:MDG27861 65:MOZ33195 66:NGA37001 67:GHA21383 68:BEN03269 69:UGA49811 70:NGA36835 71:KEN54171 72:GIN22616 73:TCD10698 74:GIN22604 75:TCD10718 76:COD13935 77:UGA49745 78:UGA49781 79:UGA49730 80:UGA49766 81:RWA38126 82:ETH18714 83:UGA49776 84:UGA49713 85:ZMB50884 86:UGA49733"
scenario=2
for i in $IUs ; do
    iu=$( echo $i | cut -f 2 -d : )
    group=$( echo $i | cut -f 1 -d : )
    CMD="time python3 -u run.py -d $short -g $group -i $iu -s $scenario -n $num_sims -m $demogName"
    echo "*--> running ${CMD}"
    unbuffer bash -c "${CMD}" | tee -a $result_folder/s${scenario}_g${group}_${iu}.out
    echo
done
