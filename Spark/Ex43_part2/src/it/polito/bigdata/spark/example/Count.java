package it.polito.bigdata.spark.example;
import java.io.Serializable;

@SuppressWarnings("serial")
public class Count implements Serializable{
	
	private int freeSlots;
	private int totalSlots;
	
	//constructor
	public Count(int free,int total) {
		this.freeSlots = free;
		this.totalSlots = total;
	}
	
	
	//getter and setters
	public int getFreeSlots(){
		return freeSlots;
	}
	public int getTotalSlots(){
		return totalSlots;
	}
	public void setFreeSlots(int value) {
		this.freeSlots = value;
	}
	public void setTotalSlots(int value) {
		this.totalSlots = value;
	}
	
	public String toString() {
		return ("freeSlots: " + this.freeSlots + "  totalSlots: " + this.totalSlots );
	}
}
