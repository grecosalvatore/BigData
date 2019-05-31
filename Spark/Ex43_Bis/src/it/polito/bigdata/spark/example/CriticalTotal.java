package it.polito.bigdata.spark.example;

import scala.Serializable;

public class CriticalTotal implements Serializable{

	int critical;
	int total;
	
	public CriticalTotal(int c,int t) {
		this.critical = c;
		this.total = t;
	}
	
	//getters and setters
	public void setCritical(int value) {
		this.critical = value;
	}
	public void setTotal	(int value) {
		this.total = value;
	}
	
	public int getCritical() {
		return this.critical;
	}
	public int getTotal() {
		return this.total;
	}
	
	public String toString() {
		return new String(""+(double)((double)this.critical/(double)this.total*100));
	}
}
