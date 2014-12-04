package de.tuberlin.dima.minidb.qexec;

import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.io.index.BTreeIndex;

public class G10IndexScanOperator implements IndexScanOperator{
	

	private BTreeIndex index;
	private DataField startKey;
	private DataField stopKey;
	private boolean startKeyIncluded;
	private boolean stopKeyIncluded;

	public G10IndexScanOperator(BTreeIndex index, DataField startKey, DataField stopKey,
				boolean startKeyIncluded, boolean stopKeyIncluded) {
		
					this.index = index;
					this.startKey = startKey;
					this.stopKey = stopKey;
					this.startKeyIncluded = startKeyIncluded;
					this.stopKeyIncluded = stopKeyIncluded;
	}
	

	@Override
	public void open(DataTuple correlatedTuple) throws QueryExecutionException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public DataTuple next() throws QueryExecutionException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() throws QueryExecutionException {
		// TODO Auto-generated method stub
		
	}
	
	
}