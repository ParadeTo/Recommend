package com.paradeto.recommend;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;



/**
 * HashMap排序
 * @author youxingzhi
 *
 */
public class SortHashMap {
	private List<Entry<String,Float>> list = new LinkedList<Entry<String,Float>>();
	/**
	 * 倒序
	 * @param map
	 * @return
	 */
	public static  List<Entry<String,Float>> sortHashMap(HashMap<String,Float> map){
		SortHashMap sorthashmap = new SortHashMap();
		
		sorthashmap.list.addAll(map.entrySet());
		
		Collections.sort(sorthashmap.list,new Comparator<Entry<String,Float>>(){
			public int compare(Entry obj1,Entry obj2){
				if(Float.parseFloat(obj1.getValue().toString())<Float.parseFloat(obj2.getValue().toString()))
					return 1;
				else if(Float.parseFloat(obj1.getValue().toString())==Float.parseFloat(obj2.getValue().toString()))
					return 0;
				else
					return -1;
			}
		});
		/*Iterator<Entry<String,Float>> ite = list.iterator();
		while(ite.hasNext()){
			Entry<String,Float> tmp = ite.next();
			System.out.println(tmp.getKey()+"\t"+tmp.getValue());
			sorthashmap.map.put(tmp.getKey(),tmp.getValue());
		}*/
		return sorthashmap.list;	
	}
	public static void main(String[]args){
		HashMap<String, Float> omap = new HashMap<String, Float> ();
		omap.put("a", (float)(1.0));
		omap.put("b", (float)(3.0));
		omap.put("c", (float)(2.0));	
		List<Entry<String,Float>> list = new LinkedList<Entry<String,Float>>();
		list=SortHashMap.sortHashMap(omap);
		for(Entry<String,Float> ilist : list){
			System.out.println(ilist.getKey()+"\t"+ilist.getValue());
		}			
	}
}
