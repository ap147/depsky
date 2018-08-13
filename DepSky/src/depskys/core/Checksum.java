package depskys.core;
import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

// https://docs.oracle.com/javase/7/docs/technotes/guides/security/StandardNames.html#MessageDigest
// http://www.rgagnon.com/javadetails/java-0416.html

public class Checksum
{

    public static byte[] createChecksum(String filename, String algorithm ) throws Exception
    {
            InputStream fis =  new FileInputStream(filename);
            byte[] buffer = new byte[1024];
            MessageDigest complete = MessageDigest.getInstance(algorithm);
            int numRead;

            do
            {
                numRead = fis.read(buffer);
                if(numRead > 0)
                {
                    complete.update(buffer, 0, numRead);
                }
            }

            while (numRead != -1);
            fis.close();

            return complete.digest();
    }

    public static String getChecksum(String filename) throws Exception
    {

        byte[] b = createChecksum(filename, "SHA1");
        String result = "";

        for (int i=0; i < b.length; i++)
        {
            result += Integer.toString( ( b[i] & 0xff ) + 0x100, 16).substring( 1 );
        }

        return result;
    }

    public static String getChecksum(byte[] data, String algorithm) throws NoSuchAlgorithmException
    {
        MessageDigest md = MessageDigest.getInstance(algorithm);
        return byteArray2Hex(md.digest(data));
    }

    private static String byteArray2Hex(final byte[] hash)
    {
        Formatter formatter = new Formatter();
        for (byte b : hash)
        {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
