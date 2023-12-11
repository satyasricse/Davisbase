import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

public class DavisBase {


	public static final String DIRECTORY = "data";
	public static final String SPACE_DELIMITER = " ";
	static String prompt = "davisql> ";
	static String copyright = "Group G";
	static String version = "v1.00";
	static boolean isExit = false;

	static Scanner scanner = new Scanner(System.in).useDelimiter(";");
	
	
    public static void main(String[] args) {
    	Initialization.init();
		
		printInitialInfo();
		String userCommand;

		while(!isExit) {
			System.out.print(prompt);
			userCommand = scanner.next().replace("\n", " ").replace("\r", "").trim().toLowerCase();
			parseUserInput(userCommand);
		}
		System.out.println("Exiting...");


	}
	
    public static void printInitialInfo() {
		System.out.println(line("-",80));
        System.out.println("Welcome to DavisBase");
		System.out.println("DavisBase Version " + version);
		System.out.println(copyright);
		System.out.println("\nType \"help;\" to display supported commands.");
		System.out.println(line("-",80));
	}
	

	
	public static String line(String s,int num) {
		String a = "";
		for(int i=0;i<num;i++) {
			a += s;
		}
		return a;
	}
	
	
	public static void help() {
		System.out.println(line("*",80));
		System.out.println("SUPPORTED COMMANDS");
		System.out.println("All commands below are case insensitive");
		System.out.println();
		System.out.println("\tSHOW TABLES;                                                 Display all the tables in the database.");
		System.out.println("\tCREATE TABLE table_name (<column_name datatype>);            Create a new table in the database.");
		System.out.println("\tINSERT INTO table_name VALUES (value1,value2,..);            Insert a new record into the table.");
		System.out.println("\tDELETE FROM TABLE table_name WHERE row_id = key_value;       Delete a record from the table whose rowid is <key_value>.");
		System.out.println("\tUPDATE table_name SET column_name = value WHERE row_id = ..; Modifies the records in the table.");
		System.out.println("\tCREATE INDEX ON table_name (column_name);                    Create index for the specified column in the table");
		System.out.println("\tSELECT * FROM table_name;                                    Display all records in the table.");
		System.out.println("\tSELECT * FROM table_name WHERE column_name operator value;   Display records in the table where the given condition is satisfied.");
		System.out.println("\tDROP TABLE table_name;                                       Remove table data and its schema.");
		System.out.println("\tVERSION;                                                     Show the program version.");
		System.out.println("\tHELP;                                                        Show this help information.");
		System.out.println("\tEXIT;                                                        Exit DavisBase.");
		System.out.println();
		System.out.println();
		System.out.println(line("*",80));
	}

	public static boolean verifyIfTableAlreadyExists(String name){
		name = name+".tbl";
		String finalName = name;
		return Arrays.stream(new File(DIRECTORY).list()).anyMatch(file -> file.equalsIgnoreCase(finalName));
	}


	public static String[] parserEquation(String equ){
		String comparator[] = new String[3];
		String temp[];
		if(equ.contains("=")) {
			temp = equ.split("=");
			comparator[0] = temp[0].trim();
			comparator[1] = "=";
			comparator[2] = temp[1].trim();
		}

		if(equ.contains("<")) {
			temp = equ.split("<");
			comparator[0] = temp[0].trim();
			comparator[1] = "<";
			comparator[2] = temp[1].trim();
	
		}
		
		if(equ.contains(">")) {
			temp = equ.split(">");
			comparator[0] = temp[0].trim();
			comparator[1] = ">";
			comparator[2] = temp[1].trim();
		}
		
		if(equ.contains("<=")) {
			temp = equ.split("<=");
			comparator[0] = temp[0].trim();
			comparator[1] = "<=";
			comparator[2] = temp[1].trim();
		}

		if(equ.contains(">=")) {
			temp = equ.split(">=");
			comparator[0] = temp[0].trim();
			comparator[1] = ">=";
			comparator[2] = temp[1].trim();
		}
		return comparator;
	}
		
	public static void parseUserInput(String inputStr) {
		
		ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(inputStr.split(SPACE_DELIMITER)));

		String command = commandTokens.get(0);
		if(command.equalsIgnoreCase("show")){
			ShowTables.showTables();
		}else if(command.equalsIgnoreCase("create")){
			CreateTable.parseCreateString(inputStr);
		}else if(command.equalsIgnoreCase("insert")){
			Insert.parseInsertString(inputStr);
		}else if(command.equalsIgnoreCase("delete")){
			DeleteTable.parseDeleteString(inputStr);
		}else if(command.equalsIgnoreCase("update")){
			UpdateTable.parseUpdateString(inputStr);
		}else if(command.equalsIgnoreCase("select")){
			parseQueryString(inputStr);
		}else if(command.equalsIgnoreCase("drop")){
			DropTable.dropTable(inputStr);
		}else if (command.equalsIgnoreCase("quit") || command.equalsIgnoreCase("exit")){
			isExit = true;
		}else{
			help();
		}

	}
    public static void parseQueryString(String queryString) {
		System.out.println("Processing Select method with input " + queryString);
		String[] cmp;
		String[] column;
		String[] temp = queryString.split("where");
		if(temp.length > 1){
			String tmp = temp[1].trim();
			cmp = parserEquation(tmp);
		}
		else{
			cmp = new String[0];
		}
		String[] select = temp[0].split("from");
		String tableName = select[1].trim();
		String cols = select[0].replace("select", "").trim();
		if(cols.contains("*")){
			column = new String[1];
			column[0] = "*";
		}
		else{
			column = cols.split(",");
			for(int i = 0; i < column.length; i++)
				column[i] = column[i].trim();
		}
		
		if(!verifyIfTableAlreadyExists(tableName)){
			System.out.println("Table "+tableName+" does not exist.");
		}
		else
		{
		    ShowTables.select(tableName, column, cmp);
		}
	}
	
	}
		


