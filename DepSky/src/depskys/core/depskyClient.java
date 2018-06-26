package depskys.core;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class depskyClient {

    static int clientID = 2;

    public static void main(String args[]) throws Exception {


        LocalDepSkySClient client = new LocalDepSkySClient(clientID,false);

        System.out.println("USAGE:  commands             function");
        System.out.println("       upload   'filename'    - uploads file");
        System.out.println("       download 'filename'    - read the last version of the selected container");
        System.out.println("       delete   'filename'    - delete all the files in the selected container");
        System.out.println("       exit                   - stop program");

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        boolean exit = false;

        while(!exit)
        {
            String s = br.readLine();

            System.out.println(s);

            if (s.equals("exit"))
            {
                exit = true;
            }

            String [] arguments = s.split(" ");

            if (arguments[0].equals("upload"))
            {
                upload(client, arguments[1]);

            }
            else if (arguments[0].equals("download"))
            {
                download(client, arguments[1]);
            }
            else if (arguments[0].equals("delete"))
            {
            }

        }
    }

    private static void upload(LocalDepSkySClient client, String filename) throws Exception {
        String hashOfFile = getHash(filename);

        if (fileExists("dataMap", hashOfFile))
        {
            // upload refrence only
            System.out.println("File already exists");
        }
        else
        {
            DepSkySDataUnit dataUnit = new DepSkySDataUnit(filename);

            byte [] data = readBytesFromFile(filename);
            client.write(dataUnit, data);

            makeEntry(1, filename, filename, hashOfFile);
        }
    }

    private static void download(LocalDepSkySClient client, String filename) throws Exception {

        if (userOwnsFile(filename))
        {
            DepSkySDataUnit dataUnit = new DepSkySDataUnit(filename);

            byte[] dataread = client.read(dataUnit);

            FileOutputStream fileOuputStream = new FileOutputStream(filename);
            fileOuputStream.write(dataread);
            fileOuputStream.close();
        }
        else
        {
            System.out.println("User does not own this file");
        }
    }

    private static void delete(LocalDepSkySClient client, String filename) throws Exception {

        DepSkySDataUnit dataUnit = new DepSkySDataUnit(filename);
        client.deleteContainer(dataUnit);

        // IF
        // another user uses this file, only delete file from current client
        // ELSE
        // delete

    }

    // Used when uploading, too check if dedup can occur
    private static boolean fileExists(String filename, String hash)
    {
        // go through file searching for this hash.
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line;
            while ((line = br.readLine()) != null) {
                // process the line.
                String [] split = line.split(" ");

                if(split.length == 4)
                {
                    if (split[3].equals(hash)){
                        if(Integer.parseInt(split[0]) == clientID)
                        {
                            return true;
                        }
                    }
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    // Used when download, to verify use owns this file.
    private static boolean userOwnsFile(String filename)
    {
        // go through file searching for this hash.
        try (BufferedReader br = new BufferedReader(new FileReader("dataMap"))) {
            String line;
            while ((line = br.readLine()) != null) {
                // process the line.
                String [] split = line.split(" ");

                if(split.length == 4)
                {
                    if (Integer.parseInt(split[0]) == clientID && split[1].equals(filename)){
                        return true;
                    }
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }


    // When file uploaded, makes entry in dataMap.
    private static void makeEntry(int clientID, String filename, String containerName, String sHash)
    {
        String entry = clientID + " " + filename + " " + containerName + " " + sHash + '\n';
        System.out.println("inserting : " + entry);

        try {
            Files.write(Paths.get("dataMap"), entry.getBytes(), StandardOpenOption.APPEND);
        }catch (IOException e) {
            //exception handling left as an exercise for the reader
        }
    }

    // Calculate Hash of File
    private static String getHash(String filename) throws Exception {
        String hash;

        com.amarjot.ChecksumSHA1 x = new com.amarjot.ChecksumSHA1();
        hash = x.getSHA1Checksum(filename);

        return hash;
    }

    private static byte[] readBytesFromFile(String filePath) {

        FileInputStream fileInputStream = null;
        byte[] bytesArray = null;

        try {

            File file = new File(filePath);
            bytesArray = new byte[(int) file.length()];

            //read file into bytes[]
            fileInputStream = new FileInputStream(filePath);
            fileInputStream.read(bytesArray);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fileInputStream != null) {
                try {
                    fileInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }

        return bytesArray;

    }
}
