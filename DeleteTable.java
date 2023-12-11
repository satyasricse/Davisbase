import java.io.RandomAccessFile;
public class DeleteTable {
	public static void parseDeleteString(String deleteString) {
		System.out.println("Processing Delete method with query" + deleteString);

		String[] tokens=deleteString.split(" ");
		String table = tokens[3];
		String[] temp = deleteString.split("where");
		String cmpTemp = temp[1];
		String[] cmp = DavisBase.parserEquation(cmpTemp);
		if(!DavisBase.verifyIfTableAlreadyExists(table)){
			System.out.println("Table "+table+" does not exist.");
		}
		else
		{
			delete(table, cmp);
		}
	}
	public static void delete(String table, String[] cmp){
		try{
		int key = new Integer(cmp[2]);

		RandomAccessFile file = new RandomAccessFile("data/"+table+".tbl", "rw");
		int numPages = Table.getNumOfPages(file);
		int page = 0;
		for(int p = 1; p <= numPages; p++)
			if(Page.hasKey(file, p, key)&Page.getPageType(file, p)==0x0D){
				page = p;
				break;
			}
		
		if(page==0)
		{
			System.out.println("The given key value does not exist");
			return;
		}
		
		short[] cellsAddr = Page.getCellArray(file, page);
		int k = 0;
		for(int i = 0; i < cellsAddr.length; i++)
		{
			long loc = Page.getCellLoc(file, page, i);
			String[] vals = Table.retrieveValues(file, loc);
			int x = new Integer(vals[0]);
			if(x!=key)
			{
				Page.setCellOffset(file, page, k, cellsAddr[i]);
				k++;
			}
		}
		Page.setCellNumber(file, page, (byte)k);
		
		}catch(Exception e)
		{
			e.printStackTrace();
		}
		
	}

}
