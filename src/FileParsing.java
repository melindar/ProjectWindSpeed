import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class FileParsing 
{

	public static void main(String[] args) 
	{
		try 
		{
			Scanner scan = new Scanner(new File("061200-99999-1981.op"));
			// Throw away header
			scan.nextLine();
			
			/* List of file variables:
			 * 
			 * STN		: Air Force Datsav3 station number
			 * WBAN		: NCDC WBAN number
			 * YEARMODA : Year Month Day  
			 * TEMP  	: Mean temperature (.1 Fahrenheit)      
			 * DEWP  	: Mean dew point (.1 Fahrenheit)     
			 * SLP   	: Mean sea level pressure (.1 mb)      
			 * STP   	: Mean station pressure (.1 mb)    
			 * VISIB 	: Mean visibility (.1 miles)     
			 * WDSP  	: Mean wind speed (.1 knots)    
			 * MXSPD 	: Maximum sustained wind speed (.1 knots) 
			 * GUST  	: Maximum wind gust (.1 knots) 
			 * MAX   	: Maximum temperature (.1 Fahrenheit)
			 * MIN   	: Minimum temperature (.1 Fahrenheit)
			 * PRCP  	: Precipitation amount (.01 inches)
			 * SNDP  	: Snow depth (.1 inches)
			 * FRSHTT	: Indicator for occurrence of:  Fog
                              						 	Rain or Drizzle
                              						 	Snow or Ice Pellets
                              						 	Hail
                              						 	Thunder
                              						 	Tornado/Funnel Cloud
			 * 
			 */
			
			String station,wban,yearmoda,temp,dewp,slp,stp,visib,wdsp,mxspd,gust,max,min,prcp,sndp,frshtt;
			int j = 0;
			while(scan.hasNextLine())
			{
				String line = scan.nextLine();
				String[] tokens = line.split("\\p{Space}+");
				// 22 variables are expected from each line
				if(tokens.length != 22)
					continue;
				
			}
			
			scan.close();
		} 
		catch (FileNotFoundException e) 
		{
			e.printStackTrace();
		}

	}

}
