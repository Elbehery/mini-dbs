package de.tuberlin.dima.minidb.io.manager;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;

import de.tuberlin.dima.minidb.io.cache.CacheableData;

public class G10ReadThread extends Thread {
	
	
	interface PrefetchCallback {
		
		void addPageInCache(int resourceId, CacheableData page, boolean pin);
	}
	
	
	public ConcurrentLinkedQueue<HashMap<EntryId, G10ReadRequest>> requests;
	
	private PrefetchCallback callback;

	private volatile boolean alive;
	
	private static final int maxSubqueueSize = 10;
	
	
	
	
	
	
	public G10ReadThread(PrefetchCallback callback) {
		
		
		this.requests = new ConcurrentLinkedQueue<HashMap<EntryId, G10ReadRequest>>();
		this.callback = callback;
		this.alive = true;
		
	}
	
	
	@Override
	public void run() {
		
		while(this.alive) {
			
			if (!requests.isEmpty()) {
				
				HashMap<EntryId, G10ReadRequest> subqueue = requests.peek();
				
				int firstPage = Integer.MAX_VALUE;
				EntryId firstEntry = null;
				G10ReadRequest request = null;
				
				
				synchronized(subqueue) {
					
					
					
					Iterator<Entry<EntryId, G10ReadRequest>> it = subqueue.entrySet().iterator();
					
					while(it.hasNext()) {
						Entry<EntryId, G10ReadRequest> entry = it.next();
						
						if (entry.getKey().getPageNumber() < firstPage) {
							firstPage = entry.getKey().getPageNumber();
							
							firstEntry = entry.getKey();
						}					
					}
					
					
					if (firstEntry != null)
						request = subqueue.get(firstEntry);
					

					
					
				}
					
				
				if(firstEntry == null) {
					System.out.println("Entry is null !"); 
				} else {
		
					synchronized (request) {
					
						ResourceManager resource = request.getManager();
						byte[] buffer = request.getBuffer();
						int pageNumber = request.getPageNumber();
						
						try {
							CacheableData page;
							
							synchronized(resource) {
								 page = resource.readPageFromResource(buffer, pageNumber);
								 
							}
							
							if (request.isPrefetch())
								callback.addPageInCache(request.getResourceId(), page, false);
							
							request.setWrapper(page);
							
						} catch (IOException ioe) {
							System.out.println("Read IO Exception : " + ioe.getMessage());
							
							
						} finally {
							
							synchronized(subqueue) {
								if (firstEntry != null) {
									subqueue.remove(firstEntry);
									if (subqueue.isEmpty())
										requests.remove(subqueue);
										
								}
							}
							
							request.done();
							
	
								request.notifyAll();						
						}
						
					}
				}
			}	
		}		
	}
	
	
	
	public void request(G10ReadRequest request) {
		
		int resourceId = request.getResourceId();
		int pageNumber = request.getPageNumber();
		
		
		Iterator<HashMap<EntryId, G10ReadRequest>> it = requests.iterator();
		
		while (it.hasNext()) {
			HashMap<EntryId, G10ReadRequest> subqueue = it.next();
			
			
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
		
		
		HashMap<EntryId, G10ReadRequest> subqueue = new HashMap<EntryId, G10ReadRequest>();
		
		synchronized(subqueue) {
			EntryId newId = new EntryId(resourceId, pageNumber);
			subqueue.put(newId, request);
		}
		requests.add(subqueue);
		
		
	}
	
	
	public G10ReadRequest getRequest(int resourceId, int pageNumber) {
		
		Iterator<HashMap<EntryId, G10ReadRequest>> it = requests.iterator();
		
		while (it.hasNext()) {
			
			HashMap<EntryId, G10ReadRequest> subqueue = it.next();
			
			synchronized(subqueue) {
			
				Iterator<Entry<EntryId, G10ReadRequest>> subIt = subqueue.entrySet().iterator();
				
				while (subIt.hasNext()) {
					Entry<EntryId, G10ReadRequest> entry = subIt.next();
					EntryId id = entry.getKey();
					
					if (id.getResourceId() == resourceId && id.getPageNumber() == pageNumber)
						return entry.getValue();
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
