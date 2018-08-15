package depskys.core;

import exceptions.StorageCloudException;

public class depskyClient {

    LocalDepSkySClient client;

    public depskyClient(int clientID, boolean saveOnCloud) throws StorageCloudException {

        client = new LocalDepSkySClient(clientID,saveOnCloud);
    }

    protected void upload(String bucketName, byte[] data) throws Exception {


        DepSkySDataUnit dataUnit = new DepSkySDataUnit(bucketName);
        dataUnit.setUsingPVSS(true);
        client.write(dataUnit, data);
    }

    protected byte[] download(String bucketName) throws Exception {


        DepSkySDataUnit dataUnit = new DepSkySDataUnit(bucketName);
        dataUnit.setUsingPVSS(true);
        byte[] dataread = client.read(dataUnit);

        return dataread;
    }

    private void delete(String bucketName) throws Exception {

        DepSkySDataUnit dataUnit = new DepSkySDataUnit(bucketName);
        client.deleteContainer(dataUnit);

    }
}
