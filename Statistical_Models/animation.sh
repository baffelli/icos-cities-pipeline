cd /project/muem/Covid-19_ZH/Results/TrafficCountMaps/

mencoder 'mf://*.jpg' -mf w=992:h=960:fps=3:type=jpg -ovc lavc -lavcopts vcodec=mpeg4:mbd=2:trell:vbitrate=5580 -oac copy -o animation.avi