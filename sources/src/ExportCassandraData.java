import java.text.SimpleDateFormat;
import java.util.Iterator;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;

/**
 *
 */
public class ExportCassandraData
{
    public static void main(String[] args)
    {
        String keyspace = args[0];
        String table = args[1];
        String username = args[2];
        String password = args[3];
        String host = args[4];
        String keytoexport = args[5];
        long start = Long.parseLong(args[6]);
        long end = Long.parseLong(args[7]);

        Cluster.Builder clusterBuilder = Cluster.builder().addContactPoints(host).withCredentials(username, password);
        Cluster cluster = clusterBuilder.build();
        Session session = cluster.connect(keyspace);

        Statement stmt = new SimpleStatement("SELECT " + keytoexport + " FROM " + table);
        stmt.setFetchSize(1000);
        ResultSet rs = session.execute(stmt);
        Iterator<Row> iter = rs.iterator();
        long rownum = 0;
        while (!rs.isFullyFetched())
        {
            rs.fetchMoreResults();
            Row row = iter.next();

            if (row != null && (rownum >= start) && (rownum <= end))
            {

                StringBuilder line = new StringBuilder();
                for (Definition key : row.getColumnDefinitions().asList())
                {
                    if (key.getName().equalsIgnoreCase(keytoexport))
                    {
                        String val = myGetValue(key, row);
                        line.append("\"");
                        line.append(val);
                        line.append("\"");
                        line.append(",");
                    }
                }
                line.deleteCharAt(line.length() - 1);
                System.out.println(line.toString());
            }
            rownum++;
        }

        session.close();
        cluster.close();

    }

    public static String myGetValue(Definition key, Row row)
    {
        String str = "";

        if (key != null)
        {
            String col = key.getName();

            try
            {
                if (key.getType() == DataType.cdouble())
                {
                    str = new Double(row.getDouble(col)).toString();
                }
                else if (key.getType() == DataType.cint())
                {
                    str = new Integer(row.getInt(col)).toString();
                }
                else if (key.getType() == DataType.uuid())
                {
                    str = row.getUUID(col).toString();
                }
                else if (key.getType() == DataType.cfloat())
                {
                    str = new Float(row.getFloat(col)).toString();
                }
                else if (key.getType() == DataType.timestamp())
                {
                    str = row.getDate(col).toString();

                    SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ");
                    str = fmt.format(row.getDate(col));

                }
                else
                {
                    str = row.getString(col);
                }
            }
            catch (Exception e)
            {
                str = "";
            }
        }

        return str;
    }

}