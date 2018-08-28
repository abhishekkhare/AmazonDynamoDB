package com.abhi.edu.dynamoDB.db;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 
 * @author Abhishek Khare
 *
 */
public class Run {
	public static final String TABLE_NAME = "MusicCollection" + 9;

	public static void main(String[] args) {
		CreateTable.create();
		AlterTable.alter();
		PutItem.put();
		PutItem.putJSON("{\"Artist\": \"Anuradha Paudwal\",\"SongTitle\": \"Tere Naam Liya\",\"AlbumTitle\": \"Ram Lakhan\",\"DateTime\": \""+new Date().toString()+"\"}");
		GetItem.getItem();
		ScanTables.getALL();
		ScanTables.getWithFilter("Artist", "Kishore Kumar");
		
		Map<String, Object> keyValueMap = new HashMap<String, Object>();
		keyValueMap.put("Artist", "Kishore Kumar");
		keyValueMap.put("SongTitle", "Neele Neele Ambar Par");
		
		Map<String, Object> updatedvalues = new HashMap<String, Object>();
		updatedvalues.put("AlbumTitle", "Kalaakaar");
		updatedvalues.put("DateTime", new Date().toString());
		try {
			UpdateItem.update(keyValueMap, updatedvalues);
			GetItem.getItem();
			Thread.sleep(3000);
			UpdateItem.updateWithJSON(keyValueMap, "{\"AlbumTitle\":\"Kalaakaar\",\"DateTime\":\""+new Date().toString()+"\"}");
			GetItem.getItem();	
		}catch(Exception e) {
			e.printStackTrace();
		}
				
		DeleteItem.delete(keyValueMap);
	}
}
