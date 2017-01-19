import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Properties;

/**
 * 
 * @author asifbashar
 *
 */
public class DistributeSSTables
{
    public static String getPathWithSlash(String path)
    {
        if (path.endsWith("/"))
        {
            return path;
        }
        else
        {
            return path + "/";
        }
    }

    public static void main(String[] args) throws FileNotFoundException, IOException
    {
        Properties prop = new Properties();
        String dbFileTypes = "-Index.db,-Statistics.db,-Summary.db,-TOC.txt,-CompressionInfo.db,-Data.db,-Filter.db,-Index.db,-Statistics.db";
        String[] dbFileTypesArray = dbFileTypes.split(",");
        String distributeHistoryFileName = "distributehistory.prop";
        if (args.length > 2)
        {
            distributeHistoryFileName = args[3];

        }
        if (new File(distributeHistoryFileName).exists())
        {
            prop.load(new FileInputStream(args[3]));
        }
        String inputDirStr = args[0];
        String outputDirsStr = args[1];
        String[] outputDirArr = outputDirsStr.split(",");
        File[] outputDirFiles = new File[outputDirArr.length];
        for (int i = 0; i < outputDirFiles.length; i++)
        {
            outputDirFiles[i] = new File(outputDirArr[i]);
        }
        File inputDir = new File(inputDirStr);
        File[] keyspaceDir = inputDir.listFiles();

        for (int i = 0; i < keyspaceDir.length; i++)
        {
            File[] tables = keyspaceDir[i].listFiles();
            for (int j = 0; j < tables.length; j++)
            {
                File[] tableFiles = tables[j].listFiles();
                for (int k = 0; k < tableFiles.length; k++)
                {
                    int outputDirIndex = 0;
                    String tableFileName = tableFiles[k].getName();
                    String keyspaceName = keyspaceDir[i].getName();
                    String tableName = tables[j].getName();
                    int startTableNoIndex = keyspaceName.length() + tableName.length() + 5;
                    if (tableFileName.length() > startTableNoIndex)
                    {
                        String tableNoStr = tableFileName.substring(startTableNoIndex);
                        String prefixTableName = tableFileName.substring(0, startTableNoIndex);
                        /**
                         * ks_iphsubmp-lines-tmp-jb-1296150-CompressionInfo.db ks_iphsubmp-lines-tmp-jb-1296150-Data.db
                         * ks_iphsubmp-lines-tmp-jb-1296150-Index.db
                         */
                        int indexOfEndOfTableNo = tableNoStr.indexOf("-");
                        if (indexOfEndOfTableNo > tableNoStr.length())
                        {

                            tableNoStr = tableFileName.substring(0, indexOfEndOfTableNo);

                            for (int l = 0; l < dbFileTypesArray.length; l++)
                            {
                                String dbFileName = prefixTableName + tableNoStr + dbFileTypesArray[l];
                                String dbFilePath = DistributeSSTables.getPathWithSlash(tables[j].getPath()) + dbFileName;
                                File sourceFile = new File(dbFilePath);
                                String destPath = prop.getProperty(dbFileName);
                                if (destPath == null || destPath.length() == 0)
                                {
                                    destPath = DistributeSSTables.getPathWithSlash(outputDirFiles[outputDirIndex].getParent());
                                    destPath = destPath + keyspaceName + File.pathSeparator + tableName + File.pathSeparator;
                                    new File(destPath).mkdirs();
                                    destPath = destPath + dbFileName;
                                    prop.put(dbFileName, destPath);
                                }

                                File destFile = new File(destPath);
                                if (!destFile.exists() || (Files.size(sourceFile.toPath()) != Files.size(destFile.toPath())))
                                {
                                    Files.copy(sourceFile.toPath(), destFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
                                }
                            }
                        }
                    }

                    if (outputDirIndex == (outputDirFiles.length - 1))
                    {
                        outputDirIndex = 0;
                    }
                    else
                    {
                        outputDirIndex++;
                    }

                }
            }
        }
        prop.save(new FileOutputStream(distributeHistoryFileName), "File containing destination path by source file name");

    }
}
