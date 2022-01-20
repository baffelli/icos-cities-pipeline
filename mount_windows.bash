#!/bin/bash

#	script to mount windows shares
#	needs to be started with sudo 

#shares=("//du-evs-03.empa.emp-eaw.ch/Abt503$")
shares=("//emp-svm-abt.empa.emp-eaw.ch/Abt503$" "//emp-svm-proj1.empa.emp-eaw.ch/Nabel$" "//emp-svm-proj1.empa.emp-eaw.ch/CarboSense$")
mounts=("/project/CarboSense/Win_G/" "/project/CarboSense/Win_K/" "/project/CarboSense/Win_CS/")
nn=${#shares[*]}

win_user="basi"
lin_user="basi"
lin_group="empa134"

usage='usage: mount_windows.bash \n
			-u unmount'

#	default options
unmount=0

#   copy command line arguments
#############################################
while [ -n "$1" ] 
do
    case "$1" in
        -u) unmount=1;;
         *) echo "unknown option $1"
            echo -e $usage
            exit -1
    esac
    shift
done


if [ $unmount -eq 0 ] 
then  
	read -s -p "Give Windows password for user ${win_user}: " pass
	echo ""

	ii=0
    while [ $ii -lt $nn ] 
    do
		#	create mount point if it does not exist yet
		[ ! -d ${mounts[$ii]} ] && mkdir ${mounts[$ii]}

		echo "Mounting: " ${shares[$ii]} "to" ${mounts[$ii]}
		sudo -s -- "mount.cifs --verbose  -o gid=${lin_group},uid=${lin_user},user=${win_user},password=${pass},noauto,noserverino  ${shares[$ii]} ${mounts[$ii]}"
		if [ $? -eq 0 ]; then 
			echo "Success"
		else  
			echo "Failed"
		fi

		let ii++
    done


else  
	ii=0
    while [ $ii -lt $nn ] 
    do
		echo "Unmounting: " ${shares[$ii]} "from" ${mounts[$ii]}
		sudo umount -v ${mounts[$ii]}

		let ii++
    done
fi



