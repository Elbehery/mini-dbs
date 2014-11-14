package de.tuberlin.dima.minidb.io.manager;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;

import de.tuberlin.dima.minidb.io.cache.CacheableData;
import de.tuberlin.dima.minidb.io.cache.PageSize;

public class G10WriteThread extends Thread {

	interface FreeBufferCallback   
	{  
	    void freeBuffer(PageSize pageSize);
	}  
	
	private static final int maxSubqueueSize = 10;
	
	public ConcurrentLinkedQueue<HashMap<EntryId, G10WriteRequest>> requests;
	
	private final FreeBufferCallback callback;

	private volatile boolean alive;
	
	public G10WriteThread(FreeBufferCallback callback) {
		
		this.callback = callback;
		this.requests = new ConcurrentLinkedQueue<HashMap<EntryId, G10WriteRequest>>();
		this.alive = true;
		
	}
	
	
	@Override
	public void run() {
		
		while(this.alive) {
			
			
			
			if (!requests.isEmpty()) {	
				
				
				HashMap<EntryId, G10WriteRequest> subqueue = requests.peek();
				
				byte[][] buffers;
				CacheableData[] wrappers;
				byte[] buffer;
				ResourceManager resource;
				
				synchronized(subqueue) {
					
					
					
					Iterator<Entry<EntryId, G10WriteRequest>> it = subqueue.entrySet().iterator();
					
					
					if (it.hasNext()) {
						Entry<EntryId, G10WriteRequest> entry = it.next();
						
						resource = entry.getValue().getManager();
						
						buffers = new byte[subqueue.size()][resource.getPageSize().getNumberOfBytes()];
						wrappers = new CacheableData[subqueue.size()];
						
						
						wrappers[0] = entry.getValue().getWrapper();
						buffer = wrappers[0].getBuffer();
						
						System.arraycopy(buffer, 0, buffers[0], 0, buffer.length);

					
					
						int i = 1;
						// TODO : pages should be ordered accordingly to their number
						while(it.hasNext()) {
							
							entry = it.next();
							
							wrappers[i] = entry.getValue().getWrapper();
							
							buffer = wrappers[i].getBuffer();
							
							System.arraycopy(buffer, 0, buffers[i], 0, buffer.length);	
							
							i++;
							
						}
						
						try {
							synchronized(resource) {
								resource.writePagesToResource(buffers, wrappers);
							}
							
						} catch (IOException ioe) {
							System.out.println("Write thread IOException : " + ioe.getMessage());
						} finally {
							callback.freeBuffer(resource.getPageSize());
						}
					
					}

					
					subqueue.clear();
					requests.remove(subqueue);
				}
			}
			
		}
		
	}
	
	
	
	public void request(G10WriteRequest request) {
		
		int resourceId = request.getResourceId();
		int pageNumber = request.getWrapper().getPageNumber();
		
		
		Iterator<HashMap<EntryId, G10WriteRequest>> it = requests.iterator();
		
		while (it.hasNext()) {
			HashMap<EntryId, G10WriteRequest> subqueue = it.next();
			
			
			synchronized(subqueue) {
				Iterator<EntryId> keys = subqueue.keySet().iterator();
				
				
				if (keys.hasNext()) {
				
					EntryId id = keys.next();
					
					if (id.getResourceId() != resourceId || subqueue.keySet().size() >= maxSubqueueSize)
						continue;
					
					EntryId newId = new EntryId(resourceId, pageNumber);
					subqueue.put(newId, request);
					
					
					return;
				}
			}
		}
		
		
		HashMap<EntryId, G10WriteRequest> subqueue = new HashMap<EntryId, G10WriteRequest>();
		
		synchronized(subqueue) {
			EntryId newId = new EntryId(resourceId, pageNumber);
			subqueue.put(newId, request);
		}
		requests.add(subqueue);
		
		
	}
	
	public CacheableData getRequest(int resourceId, int pageNumber) {
		
		Iterator<HashMap<EntryId, G10WriteRequest>> it = requests.iterator();
		
		while (it.hasNext()) {
			
			HashMap<EntryId, G10WriteRequest> subqueue = it.next();
			
			synchronized(subqueue) {
			
				Iterator<Entry<EntryId, G10WriteRequest>> subIt = subqueue.entrySet().iterator();
				
				while (subIt.hasNext()) {
					Entry<EntryId, G10WriteRequest> entry = subIt.next();
					EntryId id = entry.getKey();
					
					if (id.getResourceId() == resourceId && id.getPageNumber() == pageNumber)
						return entry.getValue().getWrapper();
				}
			}
		}

		return null;
		
	}
	
	
	public void stopThread()
	{
		this.alive = false;
	}
	
	
}
