package de.tuberlin.dima.minidb.io.manager;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

import de.tuberlin.dima.minidb.io.cache.CacheableData;
import de.tuberlin.dima.minidb.io.cache.PageSize;

public class G10WriteThread extends Thread {

	interface FreeBufferCallback   
	{  
	    void freeBuffer(PageSize pageSize);
	}  
	
	
	public ConcurrentLinkedQueue<G10WriteRequest> requests;
	
	private final FreeBufferCallback callback;

	private volatile boolean alive;
	
	public G10WriteThread(FreeBufferCallback callback) {
		
		this.callback = callback;
		this.requests = new ConcurrentLinkedQueue<G10WriteRequest>();
		this.alive = true;
		
	}
	
	
	@Override
	public void run() {
		
		while(this.alive) {
			
			
			
			if (!requests.isEmpty()) {		
				
				G10WriteRequest request = requests.remove();
				
				synchronized(request) {
				
					ResourceManager resource = request.getManager();
	
		
					CacheableData wrapper = request.getWrapper();
	
					byte[] buffer = request.getBuffer();
					
				
					
					try {
						synchronized(resource) {
							resource.writePageToResource(buffer, wrapper);
						}
						
					} catch (IOException ioe) {
						System.out.println("Write thread IOException : " + ioe.getMessage() + "(Page : " + wrapper.getPageNumber() +")");
					} finally {
						callback.freeBuffer(resource.getPageSize());
					}
					
				
				}
			}
			
		}
		
	}
	
	
	
	public void request(G10WriteRequest request) {
		requests.add(request);
	}
	
	public CacheableData getRequest(int resourceId, int pageNumber) {
		
		Iterator<G10WriteRequest> it = requests.iterator();
		while (it.hasNext()) {
			G10WriteRequest request = it.next();
			
			if (request.getResourceId() == resourceId && request.getWrapper().getPageNumber() == pageNumber)
				return request.getWrapper();
		}
		
		
		return null;
		
		
	}
	
	
	public void stopThread()
	{
		this.alive = false;
	}
	
	
}
