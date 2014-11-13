package de.tuberlin.dima.minidb.io.manager;

import de.tuberlin.dima.minidb.io.cache.CacheableData;
import de.tuberlin.dima.minidb.io.tables.G10TablePage;
import de.tuberlin.dima.minidb.io.tables.TablePage;

public class G10WriteRequest {
	
	private byte[] buffer;
	private CacheableData wrapper;
	private ResourceManager resource;
	private int resourceId;
	
	
	
	public G10WriteRequest(int resourceId, ResourceManager resource, byte[] buffer, CacheableData wrapper) {
		
		this.resource = resource;
		this.buffer = buffer;
		this.wrapper = wrapper;
		this.resourceId = resourceId;
		
		if (G10TablePage.readIntByteArray(buffer, G10TablePage.HEADER_POS_MAGIC_NUMBER) != TablePage.TABLE_DATA_PAGE_HEADER_MAGIC_NUMBER) {
			System.out.println("Write error : Magic number doesn't match");
		}
		
	}
	
	
	public ResourceManager getManager() {
		return this.resource;
	}
	
	public int getResourceId() {
		return this.resourceId;
	}
	
	public CacheableData getWrapper() {
		return this.wrapper;
	}	
	
	public byte[] getBuffer() {
		return this.buffer;
	}
	
	
	

}
