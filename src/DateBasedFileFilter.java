import java.io.File;
import java.io.FileFilter;
import java.util.Date;

/**
 * 
 * @author asifbashar
 *
 */
public class DateBasedFileFilter implements FileFilter
{

    Date olderThanDate = null;

    public DateBasedFileFilter(Date date)
    {
        olderThanDate = date;
    }

    @Override
    public boolean accept(File f)
    {
        if (f.lastModified() < olderThanDate.getTime())
        {
            return true;
        }
        else
        {
            return false;
        }

    }

}
