import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.text.DateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * 
 * @author asifbashar
 *
 */
public class MigrateBySpace
{

    public static void main(String[] args) throws Exception
    {
        FileInputStream fis = new FileInputStream(args[0]);

        Properties prop = new Properties();
        prop.load(fis);

        migrateFiles(prop);
        fis.close();

    }

    public static void migrateFiles(Properties prop) throws Exception
    {
        String dataDirs = prop.getProperty("dataDirs");
        String command = prop.getProperty("command");
        System.out.println("Command found: " + command);
        int daysInPast = Integer.parseInt(prop.getProperty("daysInPast"));
        String backup = prop.getProperty("backup");

        FileOutputStream fos = null;
        try
        {
            fos = new FileOutputStream(prop.getProperty("remove_command_file"));
        }
        catch (FileNotFoundException e1)
        {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        Date olderThanDate = new Date();
        olderThanDate.setTime(olderThanDate.getTime() - daysInPast * 24 * 60 * 60 * 1000);
        String[] dataDirList = dataDirs.split(",");
        FileSystem fs = FileSystems.getDefault();
        Path backupPath = fs.getPath(backup, InetAddress.getLocalHost().getHostName());
        backupPath.toFile().mkdirs();
        for (String dataDir : dataDirList)
        {
            File cDir = new File(dataDir);
            DateBasedFileFilter filter = new DateBasedFileFilter(olderThanDate);
            File[] files = cDir.listFiles(filter);
            for (File cFile : files)
            {
                if (command.equalsIgnoreCase("list"))
                {

                    System.out.println(cFile.getAbsolutePath() + " " + DateFormat.getDateInstance().format(cFile.lastModified()));
                }
                else if (command.equalsIgnoreCase("rm"))
                {

                    System.out.println("deleting " + cFile.getAbsolutePath());
                    cFile.delete();
                }
                else if (command.equalsIgnoreCase("migration"))
                {

                    System.out.println("copying " + cFile.getAbsolutePath());

                    try
                    {
                        if (fos == null)
                        {
                            throw new Exception("output file path is not provided , please provide remove_command_file in properties");
                        }
                        System.out.println("Copying " + cFile.getAbsolutePath() + " to " + backupPath.toAbsolutePath());

                        java.nio.file.Files.copy(fs.getPath(cFile.getAbsolutePath()), fs.getPath(backupPath.toFile().getAbsolutePath(), cFile.getName()), java.nio.file.StandardCopyOption.COPY_ATTRIBUTES,
                                java.nio.file.StandardCopyOption.REPLACE_EXISTING);

                        fos.write(("rm " + cFile.getAbsolutePath() + System.lineSeparator()).getBytes());
                    }
                    catch (IOException e)
                    {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
                else if (command.equalsIgnoreCase("migrationmove"))
                {

                    System.out.println("moving " + cFile.getAbsolutePath());

                    try
                    {
                        if (fos == null)
                        {
                            throw new Exception("output file path is not provided , please provide remove_command_file in properties");
                        }
                        System.out.println("moving " + cFile.getAbsolutePath() + " to " + backupPath.toAbsolutePath());

                        java.nio.file.Files.move(fs.getPath(cFile.getAbsolutePath()), fs.getPath(backupPath.toFile().getAbsolutePath(), cFile.getName()), java.nio.file.StandardCopyOption.REPLACE_EXISTING);

                    }
                    catch (IOException e)
                    {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }

            }

            // Arrays.sort(files, cDir);
        }
        fos.close();

    }
}
