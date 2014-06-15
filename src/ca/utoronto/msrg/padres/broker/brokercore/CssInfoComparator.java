package ca.utoronto.msrg.padres.broker.brokercore;

import java.util.Comparator;

public class CssInfoComparator implements Comparator<CssInfo>{

	@Override
	public int compare(CssInfo arg0, CssInfo arg1) {
		// TODO Auto-generated method stub
		if(arg0.getMatchingSubscriptions() > arg1.getMatchingSubscriptions())
		{
			return -1;
		}
		else if (arg0.getMatchingSubscriptions() < arg1.getMatchingSubscriptions())
		{
			return 1;
		}
		return 0;
	}
	

}
