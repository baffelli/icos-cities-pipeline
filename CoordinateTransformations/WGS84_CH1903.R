options(digits=20)
## Convert WGS lat/long (° dec) to CH y
WGS.to.CH.y <- function(lat, lng){
	## Converts degrees dec to sex
	lat <- DEC.to.SEX(lat)
	lng <- DEC.to.SEX(lng)

	## Converts degrees to seconds (sex)
	lat <- DEG.to.SEC(lat)
	lng <- DEG.to.SEC(lng)

	## Axiliary values (% Bern)
	lat_aux <- (lat - 169028.66)/10000
	lng_aux <- (lng - 26782.5)/10000
  
	## Process Y
 	y <- {600072.37 +
	211455.93 * lng_aux -
	10938.51 * lng_aux * lat_aux -
	0.36 * lng_aux * (lat_aux^2) -
	44.54 * (lng_aux^3)}
     
	return(y)
}

## Convert WGS lat/long (° dec) to CH x
WGS.to.CH.x <- function(lat, lng){

	## Converts degrees dec to sex
	lat <- DEC.to.SEX(lat)
	lng <- DEC.to.SEX(lng)

	## Converts degrees to seconds (sex)
	lat <- DEG.to.SEC(lat)
	lng <- DEG.to.SEC(lng)
  
	## Axiliary values (% Bern)
	lat_aux <- (lat - 169028.66)/10000
	lng_aux <- (lng - 26782.5)/10000

	## Process X
  	x <- {200147.07 +
	308807.95 * lat_aux + 
	3745.25 * (lng_aux^2) +
	76.63 * (lat_aux^2) -
	194.56 * (lng_aux^2) * lat_aux +
	119.79 * (lat_aux^3)}

	return(x)
}


## Convert CH y/x to WGS lat
CH.to.WGS.lat <- function (y, x){

	## Converts militar to civil and  to unit = 1000km
	## Axiliary values (% Bern)
	y_aux <- (y - 600000)/1000000
	x_aux <- (x - 200000)/1000000
  
	## Process lat
	lat <- {16.9023892 +
	3.238272 * x_aux -
	0.270978 * (y_aux^2) -
	0.002528 * (x_aux^2) -
	0.0447   * (y_aux^2) * x_aux -
	0.0140   * (x_aux^3)}
    
	## Unit 10000" to 1 " and converts seconds to degrees (dec)
	lat <- lat * 100/36
  
  	return(lat)  
}

## Convert CH y/x to WGS long
CH.to.WGS.lng <- function (y, x){

	## Converts militar to civil and  to unit = 1000km
	## Axiliary values (% Bern)
	y_aux <- (y - 600000)/1000000
	x_aux <- (x - 200000)/1000000
  
	## Process long
	lng <- {2.6779094 +
	4.728982 * y_aux +
	0.791484 * y_aux * x_aux +
	0.1306   * y_aux * (x_aux^2) -
	0.0436   * (y_aux^3)}
     
	## Unit 10000" to 1 " and converts seconds to degrees (dec)
  	lng <- lng * 100/36

	return(lng)
}


## Convert SEX DMS angle to DEC
SEX.to.DEC <- function (angle){

	## Extract DMS
	angle_chr <- as.character(angle)
	deg <- as.numeric(strsplit(angle_chr, "\\.")[[1]][1])
	min <- as.numeric(strsplit(as.character((angle-deg)*100), "\\.")[[1]][1])
	sec <- (((angle-deg)*100) - min) * 100

	## Result in degrees sex (dd.mmss)
	return(deg + (sec/60 + min)/60)
}

## Convert DEC angle to SEX DMS
DEC.to.SEX <- function(angle){

	## Extract DMS
	angle_chr <- as.character(angle)
	deg <- as.numeric(strsplit(angle_chr, "\\.")[[1]][1])
	min <- as.numeric(strsplit(as.character((angle-deg)*60), "\\.")[[1]][1])
	sec <- (((angle-deg)*60) - min) * 60

	## Result in degrees sex (dd.mmss)
  	return(deg + min/100 + sec/10000)

}

## Convert Degrees angle to seconds
DEG.to.SEC <- function(angle){

	## Extract DMS
	angle_chr <- as.character(angle)
	deg <- as.numeric(strsplit(angle_chr, "\\.")[[1]][1])
	min <- as.numeric(strsplit(as.character((angle-deg)*100), "\\.")[[1]][1])
	sec <- (((angle-deg)*100) - min) * 100;
  
	## Result in degrees sex (dd.mmss)
	return(sec + min*60 + deg*3600)

}