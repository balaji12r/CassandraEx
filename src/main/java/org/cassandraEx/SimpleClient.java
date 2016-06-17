package org.cassandraEx;

import com.datastax.driver.core.*;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * Created by a526903 on 6/13/16.
 */
public class SimpleClient {

    private Cluster cluster;
    public void connect(String node) {
        try {
            cluster = Cluster.builder()
                    .addContactPoint(node).build();
            Metadata metadata = cluster.getMetadata();
            System.out.println("Connected to cluster: %s " +
                    metadata.getClusterName());
        for ( Host host : metadata.getAllHosts() ) {
            System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n",
                    host.getDatacenter(), host.getAddress(), host.getRack());
        }

            Session session = cluster.connect("items");
            ResultSetFuture rs = session.executeAsync("select tcin from items");



            Futures.addCallback(rs, new FutureCallback<ResultSet>(){
                public void onSuccess(ResultSet result) {
                    try{Thread.sleep(1000); //pause introduced here to understand the thread execution after the main thread
                    while(result.iterator().hasNext())
                            System.out.println(result.iterator().next().getString("tcin"));
                    }catch(Exception e){System.out.println("Exception "+e);}
                }
                public void onFailure(Throwable t) {
                    System.out.println("failed");
                }
            });


            /*Row row;
            while(rs.iterator().hasNext()){
                row = rs.iterator().next();
                System.out.println(row.get("awards"));
            }*/


        }catch (Exception e){
            System.out.println("Exception Caught "+e);
        }
    }

    public void close() {
        cluster.close();
    }

    public static void main(String[] args) {
        SimpleClient client = new SimpleClient();
        client.connect("localhost");
        System.out.println("Finished");
        client.close();
    }
}
