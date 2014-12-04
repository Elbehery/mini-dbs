package de.tuberlin.dima.minidb.qexec;

import java.io.IOException;
import java.util.HashSet;

import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.io.cache.PageExpiredException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;
import de.tuberlin.dima.minidb.io.tables.PageTupleAccessException;
import de.tuberlin.dima.minidb.io.tables.TablePage;
import de.tuberlin.dima.minidb.io.tables.TableResourceManager;
import de.tuberlin.dima.minidb.io.tables.TupleIterator;

public class G10TableScanOperator implements TableScanOperator {

	private BufferPoolManager bufferPool;
	private TableResourceManager tableManager;
	private int resourceId;
	private int[] producedColumnIndexes;
	private long colBitmap;
	private int colBitmapSize;
	private LowLevelPredicate[] predicate;
	private int prefetchWindowLength;
	private int currentPageNumber;
	private TablePage currentPage;
	private TupleIterator iterator;


	public G10TableScanOperator(BufferPoolManager bufferPool, TableResourceManager tableManager, int resourceId,
			int[] producedColumnIndexes, LowLevelPredicate[] predicate,	int prefetchWindowLength) {
		
				this.bufferPool = bufferPool;
				this.tableManager = tableManager;
				this.resourceId = resourceId;
				this.producedColumnIndexes = producedColumnIndexes;
				this.predicate = predicate;
				this.prefetchWindowLength = prefetchWindowLength;
				
				this.colBitmap = 0;
				this.colBitmapSize = 0;
				HashSet<Integer> bits = new HashSet<Integer>();
				
				
				System.out.println("Columns : ");
				for (int i = 0; i < producedColumnIndexes.length; i++) {
					System.out.print(producedColumnIndexes[i] + ", ");
					
					if(!bits.contains(producedColumnIndexes[i])) {
						this.colBitmap += Math.pow(2, producedColumnIndexes[i]);
						this.colBitmapSize++;
						
						bits.add(producedColumnIndexes[i]);
						
					}
				}
				
				
				System.out.println(colBitmap);
				
				
	}

	@Override
	public void open(DataTuple correlatedTuple) throws QueryExecutionException {
		
		currentPageNumber = tableManager.getFirstDataPageNumber();
	
		try {
			
			currentPage = (TablePage) bufferPool.getPageAndPin(resourceId, currentPageNumber);
			
			iterator = currentPage.getIterator(predicate, colBitmapSize, colBitmap);
			
		
			bufferPool.prefetchPages(resourceId, currentPageNumber + 1, currentPageNumber + prefetchWindowLength);
		} catch (BufferPoolException | PageExpiredException | PageTupleAccessException | IOException e) {		
			throw new QueryExecutionException(e);
		}
	}

	@Override
	public DataTuple next() throws QueryExecutionException {
		
		DataTuple test;
		
		
		try {
			if(iterator.hasNext()) {
				return normalize(iterator.next());
				
			} else {
				while(currentPageNumber != tableManager.getLastDataPageNumber()) {
					
					currentPageNumber++;
					currentPage = (TablePage) bufferPool.getPageAndPin(resourceId, currentPageNumber);
					
					iterator = currentPage.getIterator(predicate, colBitmapSize, colBitmap);
					
					if(iterator.hasNext())
						return normalize(iterator.next());
					
				}
			}
			
			
			
			
			
		} catch (BufferPoolException | IOException | PageTupleAccessException e) {
			throw new QueryExecutionException(e);
		} 
		
		return null;
	}

	@Override
	public void close() throws QueryExecutionException {

	}
	
	
	
	private DataTuple normalize(DataTuple tuple) {
		
		DataTuple result = new DataTuple(producedColumnIndexes.length);
		
		
		int i = 0;
		int currentCol = 0;
		
		while(currentCol < colBitmapSize) {
			
			if ((colBitmap << i) % 2 == 1) {
				for(int col = 0; col < producedColumnIndexes.length; col ++) {
					
					if(producedColumnIndexes[col] == i)	
						result.assignDataField(tuple.getField(currentCol), col);
					
				}
				
				currentCol++;
			}
			
			i++;
			
		}
		
		

		
		return result;
	}

}
