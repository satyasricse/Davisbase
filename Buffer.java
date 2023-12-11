import java.util.Arrays;
import java.util.HashMap;

class Buffer{
	
	public int num_row; 
	public HashMap<Integer, String[]> content;
	public String[] columnName; 
	public int[] format; 
	
	public Buffer(){
		num_row = 0;
		content = new HashMap<>();
	}

	public void addVals(int rowid, String[] val){
		content.put(rowid, val);
		num_row = num_row + 1;
	}

	public String fix(int len, String s) {
		return String.format("%-"+(len+3)+"s", s);
	}


	public void display(String[] col){
		
		if(num_row == 0){
			System.out.println("Empty set.");
		}
		else{
			Arrays.setAll(format, i -> columnName[i].length());
			for(String[] i : content.values())
				for(int j = 0; j < i.length; j++)
					if(format[j] < i[j].length())
						format[j] = i[j].length();
			
			if(col[0].equals("*")){

				for(int l: format) {
					System.out.print(DavisBase.line("-", l+3));
				}
				
				System.out.println();
				
				for(int i = 0; i< columnName.length; i++) {
					System.out.print(fix(format[i], columnName[i])+"|");
				}
				
				System.out.println();
				
				for(int l: format) {
					System.out.print(DavisBase.line("-", l + 3));
				}
				
				System.out.println();

				for(String[] i : content.values()){
					for(int j = 0; j < i.length; j++)
						System.out.print(fix(format[j], i[j])+"|");
					System.out.println();
				}
			
			}
			else{
				int[] control = new int[col.length];
				for(int j = 0; j < col.length; j++)
					for(int i = 0; i < columnName.length; i++)
						if(col[j].equals(columnName[i]))
							control[j] = i;

				for (int k : control) System.out.print(DavisBase.line("-", format[k] + 3));
				
				System.out.println();

				for (int k : control) System.out.print(fix(format[k], columnName[k]) + "|");
				
				System.out.println();

				for (int k : control) System.out.print(DavisBase.line("-", format[k] + 3));
				
				System.out.println();
				
				for(String[] i : content.values()){
					for (int k : control) System.out.print(fix(format[k], i[k]) + "|");
					System.out.println();
				}
				System.out.println();
			}
		}
	}
}