#!/usr/bin/env bash

# 100 then 50 sims
result_folder=results
short=Hook
n=200
mkdir -p $result_folder
IUs="1:NGA36997 2:CMR07519 3:SDN53276 4:ETH18986 5:GMB47651 6:TZA46598 7:ETH19304 8:GNB23646 9:ETH18864 10:MDG27943 11:MRT31039 12:RWA38129 13:AGO02177 14:KEN54081 15:SOM56002 16:COD14016 17:GMB47641 18:GHA21393 19:COD14017 20:TZA46546 21:ETH18531 22:GNB23740 23:ERI17440 24:CIV15329 25:RWA38137 26:UGA49756 27:ERI17446 28:ZWE52929 29:UGA49749 30:SWZ45432 31:UGA49710 32:CMR07424 33:MOZ33254 34:ETH19257 35:NGA36403 36:ETH19322 37:TZA46527 38:GHA21411 39:MOZ33178 40:SSD44361 41:TZA46641 42:SSD44354 43:MDG27923 44:RWA38125 45:UGA49765 46:RWA38138 47:UGA49732 48:TZZ51896 49:MWI28953 50:UGA49805 51:UGA49758 52:KEN54212 53:UGA49707 54:SWZ45425 55:CMR07439 56:CMR07427 57:CMR07525 58:ZMB50883 59:MDG27866 60:GHA21399 61:ETH18555 62:GHA21512 63:ETH18656 64:MDG27861 65:MOZ33195 66:NGA37001 67:GHA21383 68:BEN03269 69:UGA49811 70:NGA36835 71:KEN54171 72:GIN22616 73:TCD10698 74:GIN22604 75:TCD10718 76:COD13935 77:UGA49745 78:UGA49781 79:UGA49730 80:UGA49766 81:RWA38126 82:ETH18714 83:UGA49776 84:UGA49713 85:ZMB50884 86:UGA49733 87:MDG27900 88:COD13880 89:TZZ51901 90:UGA49767 91:MDG27922 92:ZWE52940 93:KEN54276 94:MWI28948 95:MWI28950 96:KEN54227 97:SWZ45405 98:SWZ45411 99:CMR07482 100:CMR07530 101:CMR07422 102:CMR07514 103:CMR07475 104:CMR07595 105:GHA21526 106:ETH19140 107:ETH18957 108:NGA36962 109:MOZ33134 110:ETH19034 111:MOZ33203 112:GNB23719 113:MLI29998 114:ETH18954 115:ETH18818 116:NGA36833 117:ETH19122 118:ETH18936 119:NGA36789 120:UGA49755 121:UGA49768 122:UGA49743 123:MDG27856 124:UGA49762 125:UGA49782 126:RWA38117 127:UGA49729 128:UGA49744 129:ZMB53189 130:ZMB50839 131:COD13882 132:KEN54017 133:MWI28962 134:ZMB50881 135:SWZ45422 136:CMR07495 137:CMR07464 138:CMR07429 139:CMR07606 140:BFA05347 141:NGA36426 142:NGA37015 143:MOZ33156 144:ETH19281 145:UGA49799 146:RWA38112 147:MDG27879 148:TZZ51899 149:ZMB50847 150:TGO48677 151:CMR07442 152:CMR07588 153:NGA36693 154:NGA36635 155:MDG27839 156:UGA49794 157:UGA49727 158:BDI06377 159:ZMB50830 160:CMR07582 161:MDG27877 162:BDI06414 163:BDI06404 164:BDI06408 165:RWA38118 166:RWA38120 167:BDI06399 168:BDI06412 169:BDI06419 170:BDI06375"
for s in 1 2 ; do
    for i in $IUs ; do
        iu=$( echo $i | cut -f 2 -d : )
        group=$( echo $i | cut -f 1 -d : )
        unbuffer bash -c "time python3 -u run.py -d $short -i $iu -s $s -n $n -g $group" | tee -a $result_folder/$iu.out
    done
done
