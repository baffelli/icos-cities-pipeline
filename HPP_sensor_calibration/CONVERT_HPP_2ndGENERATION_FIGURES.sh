#!/bin/bash

##

for dir in "${1}/HPP_CO2_IR_CP1_pIR_SHTT_H2O_DCAL_PLF14d" 
  do
  cd $dir
  
  for scatterf in `ls *SCATTER.pdf *SCATTER_BCP.pdf`
    do
    scatterf_bn=`basename $scatterf .pdf`
    echo $scatterf_bn
    gs -sDEVICE=png16m -o ${scatterf_bn}.png -r300 ${scatterf_bn}.pdf
  done
  
done