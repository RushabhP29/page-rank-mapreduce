
package cps.prmr;

import java.util.HashMap;


public class Node {
	 private String NId;
	    private double pageRank;
	    private HashMap<String, Double> neighbourHashMap;
	    private String neighbours;
	    private boolean isDanglingNode;
	    public Node(String[] equation){
	        NId=equation[0];
	        pageRank = Double.parseDouble(equation[1]);
	        if(equation.length==2){
	            neighbours = "";
	        }
	        else{
	            neighbours = equation[2];
	        }
	        neighbourHashMap = new HashMap<>();
	        isDanglingNode = false;
	    }
	    
	    public void setDanglingBool(boolean val){
	        isDanglingNode = val;
	    }
	    
	    public boolean getDanglingBool(){
	        return isDanglingNode;
	    }
	    
	    public HashMap getHashMap(){
	        return neighbourHashMap;
	    }
	    public void putInNeighbourVal(String key, Double val){
	        neighbourHashMap.put(key, val);
	    }
	    public Double getNeighbourVal(String key){
	        return neighbourHashMap.get(key);
	    }
	    
	    public void setPageRank(double pg){
	        pageRank = pg;
	    }
	    public Double getPageRank(){
	        return pageRank;
	    }
	    
	    public void setID(String id){
	        NId = id;
	    }
	    public String getID(){
	        return NId;
	    }
	    
	    public void setNeighbours(String neighbours1){
	        neighbours = neighbours1;
	    }
	    public String getNeighbours(){
	        return neighbours;
	    }
}
