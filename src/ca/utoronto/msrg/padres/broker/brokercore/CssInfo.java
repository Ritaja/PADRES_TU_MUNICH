package ca.utoronto.msrg.padres.broker.brokercore;

public class CssInfo implements Comparable<CssInfo>{

	private String cssClass;
	private int matchingSubscriptions;
	
	public String getCssClass() {
		return cssClass;
	}

	public void setCssClass(String cssClass) {
		this.cssClass = cssClass;
	}

	public int getMatchingSubscriptions() {
		return matchingSubscriptions;
	}

	public void setMatchingSubscriptions(int matchingSubscriptions) {
		this.matchingSubscriptions = matchingSubscriptions;
	}
	
	public void incrementMatchingSubscription()
	{
		this.matchingSubscriptions++;
	}

	public CssInfo(String cssClass) {
		this.cssClass = cssClass;
	}

	@Override
	public int compareTo(CssInfo arg0) {
		int valueA = this.matchingSubscriptions;
		int valueB = arg0.matchingSubscriptions;
		if(valueA < valueB)
			return 1;
		return -1;
	}

}
