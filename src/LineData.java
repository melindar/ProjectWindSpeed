import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Scanner;

public class LineData 
{

	public static void main(String[] args) 
	{
		try {
			// Testing multiple data files. Concatenating data to output.txt for testing in Python.
			PrintWriter print = new PrintWriter(new File("output.txt"));
			//String filename = "stationdata.prn";
			URI filename = null;
			try {
				filename = new URI("stationdata.prn");
			} catch (URISyntaxException e) {
				// MapReduce requires URI object for cache files. Catching syntax errors for that URI object.
				System.out.println("Error creating the URI obejct");
			}
			LineData data = new LineData();

			// File 1 is 30750 from 1929
			Scanner scan = new Scanner(new File("030750-99999-1929.op"));
			while(scan.hasNextLine()){
				String line = scan.nextLine();
				String[] vars1 = data.processLine(line,filename);
				if(vars1 != null)
					writeFile(print,vars1);	
			}
			scan.close();
			
			// File 2 is 470990 from 2009
			Scanner scan2 = new Scanner(new File("470990-99999-2009.op"));
			while(scan2.hasNextLine()){
				String line = scan2.nextLine();
				String[] vars2 = data.processLine(line,filename);	
				if(vars2 != null)
					writeFile(print,vars2);
			}
			scan2.close();
			
			// File 3 is 010410 from 1981
			Scanner scan3 = new Scanner(new File("010410-99999-1981.op"));
			while(scan3.hasNextLine()){
				String line = scan3.nextLine();
				String[] vars3 = data.processLine(line,filename);	
				if(vars3 != null)
					writeFile(print,vars3);
			}
			scan3.close();
			
			// File 4 is 034700 from 1970
			Scanner scan4 = new Scanner(new File("034700-99999-1970.op"));
			while(scan4.hasNextLine()){
				String line = scan4.nextLine();
				String[] vars4 = data.processLine(line,filename);	
				if(vars4 != null)
					writeFile(print,vars4);
			}
			scan4.close();
			
			// File 5 is 61200 from 1981
			Scanner scan5 = new Scanner(new File("061200-99999-1981.op"));
			while(scan5.hasNextLine()){
				String line = scan5.nextLine();
				String[] vars5 = data.processLine(line,filename);	
				if(vars5 != null)
					writeFile(print,vars5);
			}
			scan5.close();
			
			// File 6 is 379050 from 2009
			Scanner scan6 = new Scanner(new File("379050-99999-2009.op"));
			while(scan6.hasNextLine()){
				String line = scan6.nextLine();
				String[] vars6 = data.processLine(line,filename);	
				if(vars6 != null)
					writeFile(print,vars6);
			}
			scan6.close();
			
			print.close();
			
		}catch (FileNotFoundException e){
			e.printStackTrace();
		}
	}
	
	/*
	 * This function is used for testing. It writes the data to a file in the specified format for post-processing. 
	 * Params: (PrintWriter) print is the open printwriter object that will write to the new output file specified by the calling function. The 
	 * 			printwriter is not closed by this function and must be closed by the caller. (String[]) vars contains all the tokens from a single line 
	 * 			in the data file.
	 */
	private static void writeFile(PrintWriter print, String[] vars)
	{
		for(int i = 0; i < vars.length; i++){
			print.print(vars[i] + "\t");
		}
		print.println();
	}
	
	/*
	 * This function gathers all the required fields from a single line of a data file into a String array. It uses the lookup file to add the location data.
	 * If the line doesn't have all fields, the function returns null. 
	 * Params: (String) line is the string from the data file that contains most of the data fields. (String) filename is the name of the lookup file that contains the location data.
	 * Return val: (String[]) an array of strings that contains each required field (one row of the input matrix), or null if either not all fields are present
	 *  			or the lookup file does not contain the location information.
	 */
	public String[] processLine(String line,URI filename)
	{
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
		 */
		
		// Throw away the file headers
		if(line.startsWith("STN"))
			return null;
		
		// Each line in the file is delimited by spaces.
		String[] tokens = line.split("\\p{Space}+");

		// There are 13 usable attributes (the second and third values are the station identifiers) and 1 target value (WDSP) 
		String[] vars = new String[16];

		// 22 variables are expected from each line, if not throw this one out
		if(tokens.length != 22)
			return null;
		else{
			//Need to collect the variables into the strings provided minus the counts (so there are only 16 variables total)
			vars[1] = tokens[0];	// station
			vars[2]= tokens[1];		// wban
			vars[3] = tokens[2];	// yearmoda
			vars[4] = tokens[3];	// temp
			vars[5] = tokens[5];	// dewp
			vars[6] = tokens[7];  	// slp
			vars[7] = tokens[9];	// stp
			vars[8] = tokens[11];	// visib
			vars[0] = tokens[13];	// WDSP
			vars[9] = tokens[17];	// max
			vars[10] = tokens[18];	// min
			vars[11] = tokens[19];	// prcp
			vars[12] = tokens[20];	// sndp

			// Handling missing values. Pressures can be estimated, other missing values require throwing out the whole line
			if(vars[5].equals("9999.9")) return null;	// Missing value do not use
			if(vars[6].equals("9999.9")) vars[6] = "1013";	// Missing value, set to standard slp
			if(vars[7].equals("9999.9")) vars[7] = "1013";	// Missing value, set to standard pressure
			if(vars[8].equals("999.9")) return null;	// Missing value do not use
			if(vars[0].equals("999.9")) return null;	// Missing value do not use (This is the target, definitely can't use!)
			if(vars[9].equals("9999.9")) return null;	// Missing value do not use
			if(vars[10].equals("9999.9")) return null;	// Missing value do not use
			if(vars[11].equals("99.99")) vars[11] = "0";	// Missing value, assumed to be 0
			if(vars[12].equals("999.9")) vars[12] = "0";	// Missing value, assumed to be 0

			// Some values have flags on the end of the field. Remove them if present.
			if(vars[9].charAt(vars[9].length()-1) == '*') 
				vars[9] = vars[9].substring(0,vars[9].length()-1);
			if(vars[10].charAt(vars[10].length()-1) == '*') 
				vars[10] = vars[10].substring(0,vars[10].length()-1);
			if(Character.isLetter(vars[11].charAt(vars[11].length()-1))) 
				vars[11] = vars[11].substring(0,vars[11].length()-1);

			// This will be used to determine the type of station identifier to be used in the lookup file.
			int station;

			// Check if there is a station number or WBAN number to identify the station.
			if(vars[1] == "99999")
				// "99999" means the WBAN is unknown, use the station ID. Index 2 in the vars array, index 1 in the files.
				station = 2;	
			else
				// WBAN is known, use it to identify the station. Index 1 in the vars array, index 0 in the files.
				station = 1;	

			// Find the latitude, longitude and elevation from the station lookup file.
			String[] location = getLatLonElev(vars[station],station-1,filename);
			if(location == null)
				// There was no location found, throw this one out.
				return null;

			// Add the location data from the lookup file to the rest of the data array.
			vars[13] = location[2];	// lat
			vars[14] = location[3];	// lon
			vars[15] = location[4];	// elev
		}
		return vars;
	}
	
	/* Get the latitude, longitude and elevation data from the stationdata.prn lookup file
	 * Params: (String) stationID is the name of the station to be found in the lookup file, (int) stationType is an index indication station or WBAN. 
	 * 			(String) filename is the name of the lookup file that contains the location data.
	 * Return val: String[] containing the row from the lookup file where the station was found, or null if it's not in the file or it doesn't have all 5 
	 * 				pieces of data (station,WBAN,lon,lat,elevation).
	 */
	private static String[] getLatLonElev(String stationID, int stationType, URI filename)
	{
		try {
			// stationIDNum is the integer form of the station in the data file (better to compare ints since sometimes values start with 0 and sometimes they don't)
			int stationIDNum = Integer.parseInt(stationID);
			
			@SuppressWarnings("resource")
			Scanner scanfile = new Scanner(new File(filename));
			
			while(scanfile.hasNextLine()){
				String line = scanfile.nextLine().trim();
				String[] tokens = line.split("\\p{Space}+");
				
				// The 5 tokens are: station,WBAN,lon,lat,elevation. If they are not all present, search for another entry that might contain all information.
				if(tokens.length != 5)
					continue;
				
				// stationNum is the integer form of the station in the station lookup file
				int stationNum = Integer.parseInt(tokens[stationType]); // stationType is 0 if it's the stationID and 1 if it's the WBAN number
				if(stationNum == stationIDNum)
					return tokens;
			}
			scanfile.close();
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			System.out.println("Error scanning stationdata.prn");
		}
		
		// The location information is not found in the lookup file, can't use this one
		return null;	
	}

}
