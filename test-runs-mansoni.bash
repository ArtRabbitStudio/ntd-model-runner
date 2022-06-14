#!/usr/bin/env bash

# 100 then 50 sims
result_folder=results/$( date +%Y%m%d%H%M%S )
short=Man
num_sims=200
demogName=KenyaKDHS
max_scenario=3
mkdir -p $result_folder
IUs="1:CAF09652 2:AGO02183 3:SSD44325 4:ETH18571 5:GNQ16421 6:AGO02087 7:CMR07446 8:ETH18884 9:NGA36600 10:SDN53305 11:COG12798 12:SSD44373 13:ETH19068 14:ZAF43269 15:KEN54253 16:NGA37077 17:ETH18892 18:MOZ33110 19:COD14225 20:MDG27851 21:ERI17451 22:ETH18618 23:UGA49802 24:UGA49799 25:GHA21427 26:SLE42259 27:KEN54254 28:ETH18754 29:NGA37014 30:MDG27838 31:COD14100 32:NER35336 33:ZWE52938 34:MLI30013 35:ETH19149 36:UGA49748 37:CMR07605 38:MOZ33171 39:ETH18657 40:LBR26822 41:NER35331 42:GHA21507 43:ETH18555 44:NGA36881 45:NER35305 46:TZZ51897 47:KEN54048 48:GHA21421 49:ETH18876 50:SEN40212 51:UGA49766 52:NGA36611 53:COD14101 54:MOZ33217 55:MOZ33212 56:TZA46620 57:NER35309 58:TZA46463 59:ETH18688 60:GHA21424 61:ETH18644 62:GHA21474 63:MLI30010 64:UGA49732 65:NGA36786 66:NGA36892 67:LBR26829 68:MWI28953 69:ETH18684 70:MOZ33202 71:MOZ33240 72:NER35325 73:NGA36876 74:NER35299 75:ERI17461 76:CMR07482 77:AGO02209 78:AGO02204 79:SEN40224 80:ETH18641 81:UGA49745 82:BDI06387 83:MOZ33200 84:MLI30030 85:MOZ33254 86:MOZ33235 87:UGA49710 88:NGA36520 89:TGO48693 90:MLI29983 91:ETH18585 92:TGO48700 93:NGA36898 94:TGO48667 95:MLI29988 96:NGA36788 97:ETH18903 98:MWI28952 99:NER35298 100:SLE42258 101:UGA49782 102:NER35303 103:UGA49725 104:TZZ51892 105:MOZ33102 106:MLI30005 107:ERI17467 108:UGA49731 109:CMR07575 110:GHA21534 111:GHA21560 112:CMR07509 113:SEN40210 114:ETH18579 115:UGA49805 116:NER35323 117:MOZ33246 118:GHA21425 119:BFA05347 120:NER35312 121:MWI28967 122:NGA36509 123:TGO48699 124:MLI29984 125:MWI28972 126:MWI28954 127:MWI28955 128:NER35327 129:MWI28948 130:TZZ51896 131:CIV15384 132:ETH18616 133:TGO48666 134:UGA49740 135:NER35304 136:MOZ33114 137:TGO48676 138:ERI17441 139:UGA49713 140:MWI28961 141:TGO48689 142:SLE42256 143:MWI28968 144:MWI28969 145:TZZ51893 146:UGA49801 147:MWI28959 148:UGA49816 149:MWI28963 150:TGO48703 151:BFA05330 152:NER35316 153:BFA05315 154:BFA05326"
for scenario in $( seq 1 $max_scenario ) ; do
    for i in $IUs ; do
        iu=$( echo $i | cut -f 2 -d : )
        group=$( echo $i | cut -f 1 -d : )
        unbuffer bash -c "time python3 -u run.py -d $short -g $group -i $iu -s $scenario -n $num_sims -m $demogName" | tee -a $result_folder/s${scenario}_g${group}_${iu}.out
    done
done