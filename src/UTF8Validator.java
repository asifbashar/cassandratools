import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.Arrays;

public class UTF8Validator
{

    /**
     * 
     * @author asifbashar
     *
     */
    public static void main(String[] args) throws IOException
    {
        FileInputStream fis = new FileInputStream(args[0]);
        byte[] b = new byte[64000];
        boolean valid = true;
        int read = 64000;
        System.out.println("validating file " + args[0]);
        while (read == 64000)
        {

            read = fis.read(b);
            byte[] cb = Arrays.copyOf(b, read);
            valid = isValidUTF8(cb);

        }
        if (valid == false)
        {
            System.out.println("invalid UTF-8");
        }
        else
        {
            System.out.println("valid UTF-8");
        }

    }

    public static boolean isValidUTF8(byte[] input)
    {

        CharsetDecoder cs = Charset.forName("UTF-8").newDecoder();

        try
        {
            cs.decode(ByteBuffer.wrap(input));
            return true;
        }
        catch (CharacterCodingException e)
        {
            return false;
        }
    }

}
