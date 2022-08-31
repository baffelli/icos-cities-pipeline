#!/bin/bash

##

density=330

for dir in "${1}/HPP_CO2_IR_CP1_pIR_H2O/" "${1}//HPP_CO2_IR_CP1_pIR/" "${1}//HPP_CO2_IR_CP1_pIR_HPPT/" 
do
  cd $dir
  convert -resize 740x740  -density $density -quality 100 SU342_S342_02_SCATTER_BCP.pdf SU342_S342_02_SCATTER_BCP.png
  convert -resize 740x740  -density $density -quality 100 SU390_S390_02_SCATTER_BCP.pdf SU390_S390_02_SCATTER_BCP.png
  convert -resize 740x740  -density $density -quality 100 SU342_S342_02_SCATTER.pdf     SU342_S342_02_SCATTER.png
  convert -resize 740x740  -density $density -quality 100 SU390_S390_02_SCATTER.pdf     SU390_S390_02_SCATTER.png
  convert -resize 740x1110 -density $density -quality 100 SU342_S342_02_BOTTLE_CAL.pdf  SU342_S342_02_BOTTLE_CAL.png
  convert -resize 740x1110 -density $density -quality 100 SU390_S390_02_BOTTLE_CAL.pdf  SU390_S390_02_BOTTLE_CAL.png
done