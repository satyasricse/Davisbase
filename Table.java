import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.stream.Collectors;

public class Table{
	
	public static int PAGE_SIZE = 512;
	public static String DATE_PATTERN = "yyyy-MM-dd_HH:mm:ss";

	public static void main(String[] args){}

	public static int getNumOfPages(RandomAccessFile file){
		int numOfPages = 0;
		try{
			numOfPages = (int)(file.length()/((long) PAGE_SIZE));
		}catch(Exception e){
			e.printStackTrace();
		}

		return numOfPages;
	}

	public static String[] getColName(String table){ //tables=davisbase_tables
		String[] cols = new String[0];
		try{
			RandomAccessFile file = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			Buffer buffer = new Buffer();
			String[] columnName = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			String[] cmp = {"table_name","=",table};
			filterColumns(file, cmp, columnName, buffer);
			HashMap<Integer, String[]> content = buffer.content;
			ArrayList<String> array = content.values().stream().map(i -> i[2]).collect(Collectors.toCollection(ArrayList::new));
			int size=array.size();
			cols = array.toArray(new String[size]);
			file.close();
			return cols;
		}catch(Exception e){
			e.printStackTrace();
		}
		return cols;
	}
	
	public static String[] getDataType(String table){
		String[] dataType = new String[0];
		try{
			RandomAccessFile file = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			Buffer buffer = new Buffer();
			String[] columnNames = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			String[] cmpStrArr = {"table_name","=",table};
			filterColumns(file, cmpStrArr, columnNames, buffer);
			HashMap<Integer, String[]> contentMap = buffer.content;
			ArrayList<String> array = contentMap.values().stream().map(x -> x[3]).collect(Collectors.toCollection(ArrayList::new));
			int size=array.size();
			dataType = array.toArray(new String[size]);
			file.close();
			return dataType;
		}catch(Exception e){
			e.printStackTrace();
		}
		return dataType;
	}

	
	public static void filterColumns(RandomAccessFile file, String[] cmp, String[] columnName, Buffer buffer){
		try{
			int numOfPages = getNumOfPages(file);
			for(int page = 1; page <= numOfPages; page++){
				file.seek((page-1)* PAGE_SIZE);
				byte pageType = file.readByte();
				if(pageType == 0x0D)
				{
					byte numOfCells = Page.getCellNumber(file, page); //accesses file header to get number of cells.
					for(int i=0; i < numOfCells; i++){
						long loc = Page.getCellLoc(file, page, i);
						String[] vals = retrieveValues(file, loc);
						int rowid=Integer.parseInt(vals[0]);
						boolean check = cmpCheck(vals, rowid, cmp, columnName);
						if(check)
							buffer.addVals(rowid, vals);
					}
				}
			}
			buffer.columnName = columnName;
			buffer.format = new int[columnName.length];

		}catch(Exception e){
			System.out.println("Error at filter");
			e.printStackTrace();
		}

	}

		public static String[] retrieveValues(RandomAccessFile file, long loc){
		
		String[] values = null;
		try{
			
			SimpleDateFormat dateFormat = new SimpleDateFormat (DATE_PATTERN);

			file.seek(loc+2);
			int key = file.readInt();
			int num_cols = file.readByte();
			
			byte[] stc = new byte[num_cols];
			file.read(stc);
			
			values = new String[num_cols+1];
			
			values[0] = Integer.toString(key);
			
			for(int i=1; i <= num_cols; i++){
				setData(file, values, dateFormat, stc, i);
			}

		}catch(Exception e){
			e.printStackTrace();
		}

		return values;
	}

	private static void setData(RandomAccessFile file, String[] values, SimpleDateFormat dateFormat, byte[] stc, int i) throws IOException {
		switch(stc[i -1]){
			case 0x00:  file.readByte();
						values[i] = "null";
						break;

			case 0x01:  file.readShort();
						values[i] = "null";
						break;

			case 0x02:  file.readInt();
						values[i] = "null";
						break;

			case 0x03:  file.readLong();
						values[i] = "null";
						break;

			case 0x04:  values[i] = Integer.toString(file.readByte());
						break;

			case 0x05:  values[i] = Integer.toString(file.readShort());
						break;

			case 0x06:  values[i] = Integer.toString(file.readInt());
						break;

			case 0x07:  values[i] = Long.toString(file.readLong());
						break;

			case 0x08:  values[i] = String.valueOf(file.readFloat());
						break;

			case 0x09:  values[i] = String.valueOf(file.readDouble());
						break;

			case 0x0A:  long temp = file.readLong();
						Date dateTime = new Date(temp);
						values[i] = dateFormat.format(dateTime);
						break;

			case 0x0B:  temp = file.readLong();
						Date date = new Date(temp);
						values[i] = dateFormat.format(date).substring(0,10);
						break;

			default:    int len = stc[i - 1] - 0x0C;
						byte[] bytes = new byte[len];
						file.read(bytes);
						values[i] = new String(bytes);
						break;
		}
	}

	public static int calculatePayloadSize(String table, String[] vals, byte[] stc){
		String[] dataType = getDataType(table);
		int size =dataType.length;
		int i = 1;
		while (i < dataType.length) {
			stc[i - 1]= getStc(vals[i], dataType[i]);
			size = size + fieldLength(stc[i - 1]);
			i++;
		}
		return size;
	}
	
	public static byte getStc(String value, String dataType){
		if(value.equals("null")){
			switch(dataType){
				case "SMALLINT":    return 0x01;
				case "INT":
				case "REAL":
					return 0x02;
				case "BIGINT":
				case "DOUBLE":
				case "DATETIME":
				case "DATE":
				case "TEXT":
					return 0x03;
				case "TINYINT":
				default:			return 0x00;
			}							
		}else{
			switch(dataType){
				case "TINYINT":     return 0x04;
				case "SMALLINT":    return 0x05;
				case "INT":			return 0x06;
				case "BIGINT":      return 0x07;
				case "REAL":        return 0x08;
				case "DOUBLE":      return 0x09;
				case "DATETIME":    return 0x0A;
				case "DATE":        return 0x0B;
				case "TEXT":        return (byte)(value.length()+0x0C);
				default:			return 0x00;
			}
		}
	}
	
    public static short fieldLength(byte stc){
		switch(stc){
			case 0x00:
			case 0x04:
				return 1;
			case 0x01:
			case 0x05:
				return 2;
			case 0x02:
			case 0x06:
			case 0x08:
				return 4;
			case 0x03:
			case 0x07:
			case 0x09:
			case 0x0A:
			case 0x0B:
				return 8;
			default:   return (short)(stc - 0x0C);
		}
	}


	
public static int searchKeyPage(RandomAccessFile file, int key){
		int val = 1;
		try{
			int numPages = getNumOfPages(file);
			for(int page = 1; page <= numPages; page++){
				file.seek((page - 1)* PAGE_SIZE);
				byte pageType = file.readByte();
				if(pageType == 0x0D){
					int[] keys = Page.getKeyArray(file, page);
					if(keys.length == 0)
						return 0;
					int rm = Page.getRightMost(file, page);
					if(keys[0] <= key && key <= keys[keys.length - 1]){
						return page;
					}else if(rm == 0 && keys[keys.length - 1] < key){
						return page;
					}
				}
			}
		}catch(Exception e){
			e.printStackTrace();
		}

		return val;
	}

	
	public static String[] getNullable(String table){
		String[] nullable = new String[0];
		try{
			RandomAccessFile file = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			Buffer buffer = new Buffer();
			String[] columnName = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			String[] cmp = {"table_name","=",table};
			filterColumns(file, cmp, columnName, buffer);
			HashMap<Integer, String[]> content = buffer.content;
			ArrayList<String> array = new ArrayList<>();
			for(String[] i : content.values()){
				array.add(i[5]);
			}
			int size=array.size();
			nullable = array.toArray(new String[size]);
			file.close();
			return nullable;
		}catch(Exception e){
			e.printStackTrace();
		}
		return nullable;
	}


	public static void filterColumns(RandomAccessFile file, String[] cmp, String[] columnName, String[] type, Buffer buffer){
		try{
			
			int numOfPages = getNumOfPages(file);
			
			for(int page = 1; page <= numOfPages; page++){
				
				file.seek((page-1)* PAGE_SIZE);
				byte pageType = file.readByte();
				
					if(pageType == 0x0D){
						
					byte numOfCells = Page.getCellNumber(file, page);

					 for(int i=0; i < numOfCells; i++){
						long loc = Page.getCellLoc(file, page, i);
						String[] vals = retrieveValues(file, loc);
						int rowid=Integer.parseInt(vals[0]);
						
						for(int j=0; j < type.length; j++)
							if(type[j].equals("DATE") || type[j].equals("DATETIME"))
								vals[j] = "'"+vals[j]+"'";
						
						boolean check = cmpCheck(vals, rowid , cmp, columnName);

						
						for(int j=0; j < type.length; j++)
							if(type[j].equals("DATE") || type[j].equals("DATETIME"))
								vals[j] = vals[j].substring(1, vals[j].length()-1);

						if(check)
							buffer.addVals(rowid, vals);
					 }
				   }
			}

			buffer.columnName = columnName;
			buffer.format = new int[columnName.length];

		}catch(Exception e){
			System.out.println("Error at filter");
			e.printStackTrace();
		}

	}

	
	public static boolean cmpCheck(String[] values, int rowid, String[] cmp, String[] columnName){

		boolean check = false;
		
		if(cmp.length == 0){
			check = true;
		}
		else{
			int colPos = 1;
			for(int i = 0; i < columnName.length; i++){
				if(columnName[i].equals(cmp[0])){
					colPos = i + 1;
					break;
				}
			}
			
			if(colPos == 1){
				int val = Integer.parseInt(cmp[2]);
				String operator = cmp[1];
				switch(operator){
					case "=":
						check = rowid == val;
							  break;
					case ">":
						check = rowid > val;
							  break;
					case ">=":
						check = rowid >= val;
					          break;
					case "<":
						check = rowid < val;
							  break;
					case "<=":
						check = rowid <= val;
							  break;
					case "!=":
						check = rowid != val;
							  break;						  							  							  							
				}
			}else{
				check = cmp[2].equals(values[colPos - 1]);
			}
		}
		return check;
	}
	
}


