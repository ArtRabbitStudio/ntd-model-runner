#!/usr/bin/env bash

# 100 then 50 sims
result_folder=results/$( date +%Y%m%d%H%M%S )
short=Tri
num_sims=200
demogName=KenyaKDHS
max_scenario=3
mkdir -p $result_folder
IUs="1:ETH18914 2:SSD44322 3:KEN54232 4:MDG27855 5:RWA38141 6:GIN22634 7:COD14145 8:TZA46463 9:SDN53203 10:ETH18813 11:TZA46565 12:COD13953 13:MDG27843 14:SDN53277 15:NGA36978 16:COD14126 17:GNQ16414 18:COD14137 19:ZMB53171 20:ETH18574 21:ETH18512 22:COD14307 23:ERI17440 24:CIV15337 25:UGA49728 26:UGA49760 27:ERI17446 28:ZWE52929 29:UGA49820 30:LSO25811 31:UGA49710 32:CMR07452 33:ETH18631 34:ETH18546 35:UGA49812 36:ETH19307 37:TZA46593 38:CAF09655 39:MOZ33101 40:BEN03230 41:MOZ33221 42:ZAF43271 43:MDG27880 44:RWA38127 45:UGA49765 46:RWA38138 47:UGA49738 48:ERI17444 49:KEN54016 50:UGA49805 51:UGA49816 52:KEN54212 53:UGA49707 54:SWZ45425 55:CMR07439 56:CMR07427 57:CMR07433 58:ZMB50878 59:MDG27866 60:MDG27929 61:ETH18555 62:GHA21528 63:ETH18656 64:GHA21394 65:TZA46497 66:NGA37001 67:AGO02117 68:BFA05362 69:UGA49811 70:NGA36835 71:KEN54063 72:TZA46636 73:ETH18651 74:MOZ33144 75:COD14198 76:NGA36588 77:UGA49746 78:UGA49706 79:UGA49730 80:UGA49766 81:RWA38114 82:ETH18714 83:UGA49754 84:UGA49713 85:ZMB50884 86:UGA49733 87:MDG27904 88:COD13870 89:TZZ51901 90:UGA49767 91:MDG27922 92:ZWE52940 93:KEN54013 94:MWI28948 95:MWI28950 96:KEN54027 97:SWZ45409 98:SWZ45408 99:CMR07482 100:CMR07530 101:CMR07435 102:CMR07504 103:CMR07475 104:CMR07595 105:GHA21527 106:ETH19140 107:GHA21471 108:SEN40198 109:COD14032 110:ETH19251 111:MOZ33203 112:NGA36936 113:MLI29985 114:ETH18954 115:ETH18818 116:NGA36605 117:ETH19122 118:NGA36591 119:NGA36507 120:UGA49755 121:UGA49768 122:UGA49743 123:MDG27838 124:UGA49762 125:UGA49782 126:RWA38117 127:UGA49729 128:UGA49797 129:ZMB53189 130:ZMB53178 131:COD13881 132:KEN54099 133:MWI28971 134:ERI17459 135:SWZ45406 136:CMR07492 137:CMR07544 138:CMR07469 139:CMR07503 140:GHA21397 141:NGA36651 142:NGA36865 143:NGA36558 144:GIN22612 145:UGA49799 146:RWA38112 147:MDG27879 148:TZZ51899 149:ZMB50847 150:TGO48678 151:CMR07571 152:CMR07588 153:CIV15351 154:ETH18814 155:MDG27901 156:UGA49794 157:UGA49727 158:BDI06377 159:ZMB50824 160:CMR07441 161:MDG27877 162:BDI06414 163:BDI06404 164:BDI06400 165:RWA38118 166:RWA38120 167:BDI06399 168:BDI06386 169:BDI06419 170:BDI06389"
for scenario in $( seq 1 $max_scenario ) ; do
    for i in $IUs ; do
        iu=$( echo $i | cut -f 2 -d : )
        group=$( echo $i | cut -f 1 -d : )
        unbuffer bash -c "time python3 -u run.py -d $short -g $group -i $iu -s $scenario -n $num_sims -m $demogName" | tee -a $result_folder/s${scenario}_g${group}_${iu}.out
    done
done
