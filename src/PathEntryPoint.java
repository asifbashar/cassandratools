import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.json.JSONObject;

@Path("/rest")
public class PathEntryPoint
{
    @GET
    @Path("test")
    @Produces(MediaType.TEXT_PLAIN)
    public String test()
    {
        return "Test";
    }

    @GET
    @Path("datafiles")
    @Produces(MediaType.APPLICATION_JSON)
    public String datafiles(@QueryParam("keyspace") String keyspace, @QueryParam("table") String table)
    {
        JSONObject json = new JSONObject();

        // String dataDir = CassandraData.getDataDir(keyspace, table);
        return "datafilelist for keyspace " + keyspace + " table " + table;
    }

}
