import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

public class CassandraData

{

    public static List<String> getDataDir(String keyspace, String table)
    {

        List<String> dataDir = new ArrayList<String>();

        ResultSet rs_cfid = DataMigrator.getSession().execute("select cf_id from system.schema_columnfamilies where keyspace_name = '" + keyspace + "' and columnfamily_name='" + table + "';");
        Row row = rs_cfid.one();
        if (row != null)
        {
            String cfid = row.getString("cf_id");
            if (cfid != null)
            {

            }
        }

        return dataDir;

    }

}
