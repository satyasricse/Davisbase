import java.io.RandomAccessFile;
public class ShowTables{

public static void showTables() {
		System.out.println("SHOW ALL TABLES");
		System.out.println("DML command:\"SHOW TABLES;\"");
		
		String table = "davisbase_tables";
		String[] tableNames = {"table_names"};
		String[] cmptr = new String[0];
		select(table, tableNames, cmptr); //Table.java having method select(davisbase_tables,table_name,=);
	}

public static void select(String table, String[] cols, String[] cmp){     //select(davisbase_tables,table_name,=)
	try{
		
		RandomAccessFile file = new RandomAccessFile("data/"+table+".tbl", "rw");
		String[] columnName = Table.getColName(table);
		String[] type = Table.getDataType(table);
		
		Buffer buffer = new Buffer();
		
		Table.filterColumns(file, cmp, columnName, type, buffer);
		buffer.display(cols);
		file.close();
	}catch(Exception e){
		System.out.println(e);
	}
}

}
