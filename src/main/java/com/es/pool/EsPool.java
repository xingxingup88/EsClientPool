package com.es.pool;

import java.io.IOException;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

public class EsPool extends GenericObjectPool<RestHighLevelClient>{
	private EsPool(String h, short port, String u, String p) {
		super(new EsPooledFactory(h, port, u, p), new GenericObjectPoolConfig());
	}
	
	public RestHighLevelClient getClient() throws Exception
	{
		return borrowObject();
	}
	
	public static class EsPooledFactory implements PooledObjectFactory<RestHighLevelClient>{
		private String host;
		private short port;
		private String user;
		private String password;
		public EsPooledFactory(String h, short port, String u, String p)
		{
			host = h;
			this.port = port;
			user = u;
			password = p;
		}
		public PooledObject<RestHighLevelClient> makeObject() throws Exception {
			CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
			credentialsProvider.setCredentials(AuthScope.ANY,
			        new UsernamePasswordCredentials(user, password));

			RestClientBuilder builder = RestClient.builder(new HttpHost(host, port))
			        .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
			            @Override
			            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
			                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
			            }
			        });
			RestHighLevelClient client = new RestHighLevelClient(builder);
			return new DefaultPooledObject<RestHighLevelClient>(client);
		}
		public void destroyObject(PooledObject<RestHighLevelClient> p)
				throws Exception {
			p.getObject().close();
		}
		public boolean validateObject(PooledObject<RestHighLevelClient> p) {
			try {
				return p.getObject().ping();
			} catch (IOException e) {
				e.printStackTrace();
			}
			return false;
		}
		public void activateObject(PooledObject<RestHighLevelClient> p)
				throws Exception {
			p.getObject().ping();
			
		}
		public void passivateObject(PooledObject<RestHighLevelClient> p)
				throws Exception {
			// TODO Auto-generated method stub
			
		}
	}
}

