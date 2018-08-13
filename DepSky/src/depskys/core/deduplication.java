package depskys.core;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.util.Arrays;

public class deduplication {

    static Connection con = null;

    static depskyClient client;
    public static void main(String[] args) throws Exception {

        String save = "save";
        String load = "load";

        Class.forName("com.mysql.cj.jdbc.Driver");
        // Make sure your Amazon rds is public available and you have added ur ip to a security group or create one.
        // More info : https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_WorkingWithSecurityGroups.html
        con = DriverManager.getConnection("jdbc:mysql://aws-rds-endpoint","username","password");

        client = new depskyClient(6, true);

        // Save
        if (args[0].equals(save))
        {
            int chunkSize = Integer.parseInt(args[1]);
            save(args[2], chunkSize);
        }
        // Load
        else if (args[0].equals(load))
        {
            load(args[1]);
        }
    }

    private static void save (String filename, int chunkSize) throws Exception {

        // Read the file into a byte array
        byte [] data = readBytesFromFile(filename);

        // Stores hashs used to make up original file
        String skeletonfile = filename + "dup";
        createEmptyFile(skeletonfile);

        byte [] dataChunk;
        int currentPoint = 0;
        int lengthData = data.length;

        int from = currentPoint;
        int to = chunkSize;

        String chunkName;

        while (currentPoint != lengthData)
        {
            chunkName = filename + currentPoint;

            // When not enough bytes for a whole chunk
            if (NotEnoughBytes(lengthData, currentPoint, chunkSize))
            {
                // Make a chunk with left over bytes
                int leftOver = lengthData - currentPoint;
                dataChunk = Arrays.copyOfRange(data, from, from + leftOver);
                saveChunk(dataChunk, skeletonfile);
                break;
            }
            else
            {
                dataChunk = Arrays.copyOfRange(data, from, to);
                saveChunk(dataChunk, skeletonfile);
            }

            currentPoint = currentPoint + chunkSize;

            from = currentPoint;
            to = currentPoint + (chunkSize);
        }
        byte [] skeletonfileInBytes = readBytesFromFile(skeletonfile);
        client.upload(skeletonfile , skeletonfileInBytes);
    }

    private static boolean NotEnoughBytes(int length, int pointer, int chunkSize)
    {
        int result = pointer + chunkSize;
        if (result > length)
        {
            return true;
        }
        return false;
    }


    private static void load (String filename) throws Exception {
        byte [] chunk, combinedChunks, tempCombinedChunks;
        String refrenceFileName = filename + "dup";
        byte [] refFile = client.download(refrenceFileName);
        byteArrayToFile(refrenceFileName, refFile);
        try (BufferedReader br = new BufferedReader(new FileReader(refrenceFileName)))
        {
            String line = br.readLine();
            combinedChunks = client.download(line);;

            while ((line = br.readLine()) != null)
            {
                //chunk = readBytesFromFile(line);
                chunk = client.download(line);
                tempCombinedChunks = new byte [combinedChunks.length + chunk.length];

                System.arraycopy(combinedChunks, 0, tempCombinedChunks, 0, combinedChunks.length);
                System.arraycopy(chunk, 0, tempCombinedChunks, combinedChunks.length, chunk.length );

                combinedChunks = tempCombinedChunks;
            }
            byteArrayToFile(filename, combinedChunks);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static void byteArrayToFile(String filename, byte [] fullFile)
    {
        // https://stackoverflow.com/questions/13352972/convert-file-to-byte-array-and-vice-versa
        String strFilePath = filename;
        try {
            FileOutputStream fos = new FileOutputStream(strFilePath);

            fos.write(fullFile);
            fos.close();
        }
        catch(FileNotFoundException ex)   {
            System.out.println("FileNotFoundException : " + ex);
        }
        catch(IOException ioe)  {
            System.out.println("IOException : " + ioe);
        }
    }
    public static void saveChunk (byte [] dataSplit, String skeletonfile) throws Exception {
        // Calculate Hash
        String hash = calculateHash(dataSplit);
        //String chunkExists = hashExists ("globalHashs", hash);
        boolean chunkExists = rdsHashExists(hash);
        String msg;

        // IF chunk doesnt exist
        if (!chunkExists)
        {
            // Write this new chunk to a file & ADD HASH TO Global hashfile

            //write(chunkName, dataSplit);
            System.out.println(hash);
            client.upload(hash , dataSplit);

//            msg = hash + " " + chunkName + " " + 1;
//            addToFile("globalHashs", msg);
            rdsWriteHash(hash);
            addToFile(skeletonfile, hash);

        }
        // OTHERWISE
        else
        {
            addToFile(skeletonfile, hash);
        }
    }

    private static void write (String filename, byte [] data) throws IOException
    {
        FileOutputStream stream = new FileOutputStream(filename);
        try
        {
            stream.write(data);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        finally
        {
            stream.close();
        }
    }

    private static boolean createEmptyFile (String filename) throws IOException
    {
        Path path = Paths.get(filename);

        try
        {
            Files.createFile(path);
            return false;
        }
        catch (FileAlreadyExistsException e)
        {
            //System.err.println("already exists: " + e.getMessage());
            return true;
        }
    }

    private static void appendToGlobalHash (String filename, byte [] data) throws IOException
    {

    }

    private static byte[] readBytesFromFile(String filePath) {

        FileInputStream fileInputStream = null;
        byte[] bytesArray = null;

        try {

            File file = new File(filePath);
            bytesArray = new byte[(int) file.length()];
            System.out.println(filePath);
            //read file into bytes[]
            fileInputStream = new FileInputStream(filePath);
            fileInputStream.read(bytesArray);

        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        finally
        {
            if (fileInputStream != null)
            {
                try
                {
                    fileInputStream.close();
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
            }

        }

        return bytesArray;
    }


    private static String calculateHash (byte [] data) throws Exception
    {
        Checksum x = new Checksum();
        return x.getChecksum(data, "SHA1");
    }

    private static String hashExists (String filename, String hash)
    {
        try (BufferedReader br = new BufferedReader(new FileReader(filename)))
        {
            String line;
            while ((line = br.readLine()) != null)
            {
                String array[] = line.split(" ");
                if (array[0].equals(hash))
                {
                    return array[1];
                }
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        return "0";
    }

    // Used when chunk is being used for multiple files
    private static void incrementHash(String filename, String hash)
    {
       // https://stackoverflow.com/questions/20039980/java-replace-line-in-text-file
    }

    // When file uploaded, makes entry in dataMap.
    private static void addToFile(String filename, String data)
    {
        String entry = data + '\n';

        try
        {
            Files.write(Paths.get(filename), entry.getBytes(), StandardOpenOption.APPEND);
        }
        catch (IOException e)
        {
            //exception handling left as an exercise for the reader
        }
    }

    // Check if hash exists in database
    public static boolean rdsHashExists(String hash) throws SQLException {

        System.out.println("- Checking if '" + hash + "' exists in database.");

        Statement sqlState = con.createStatement();
        String pickDB = "use peanut";
        String selectStuff = "SELECT * from hash_tbl WHERE hash = '"+ hash +"'";

        // Pick the database then run the query
        ResultSet rows = sqlState.executeQuery(pickDB);
        rows = sqlState.executeQuery(selectStuff);

        // if hash exists, print it and return true
        if(rows.next()) {

            System.out.println(rows.getString("hash"));
            System.out.println("- hash exists.");
            return true;
        }

        System.out.println("- hash does not exist.");
        return false;
    }

    public static void rdsWriteHash (String hash) throws SQLException {

        System.out.println("- Inserting '" + hash + "' into database." );
        Statement sqlState = con.createStatement();
        String pickDB = "use peanut";
        String selectStuff = "INSERT INTO hash_tbl (hash) VALUES ('"+ hash + "')";

        sqlState.executeQuery(pickDB);
        sqlState.executeUpdate(selectStuff);

        System.out.println("- '" + hash + "' successfully inserted.");
    }
}
