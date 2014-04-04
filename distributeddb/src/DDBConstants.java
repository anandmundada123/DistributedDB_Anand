package distributeddb;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Constants used in both Client and Application Master
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class DDBConstants {


  /**
   * Environment key name pointing to the shell script's location
   */
  public static final String DDB_DB_LOCATION = "DISTRIBUTEDDATABASE_DB_LOCATION";
  public static final String DDB_WRAP_LOCATION = "DISTRIBUTEDDATABASE_WRAP_LOCATION";
  //public static final String DDB_CONTAINER_LOCATION = "DISTRIBUTEDDATABASE_CONTAINER_LOCATION";

  /**
   * Environment key name denoting the file timestamp for the shell script. 
   * Used to validate the local resource. 
   */
  public static final String DDB_DB_TIMESTAMP = "DISTRIBUTEDDATABASE_DB_TIMESTAMP";
  public static final String DDB_WRAP_TIMESTAMP = "DISTRIBUTEDDATABASE_WRAP_TIMESTAMP";
 // public static final String DDB_CONTAINER_TIMESTAMP = "DISTRIBUTEDDATABASE_CONTAINER_TIMESTAMP";

  /**
   * Environment key name denoting the file content length for the shell script. 
   * Used to validate the local resource. 
   */
  public static final String DDB_DB_LEN = "DISTRIBUTEDDATABASE_DB_LEN";
  public static final String DDB_WRAP_LEN = "DISTRIBUTEDDATABASE_WRAP_LEN";
  //public static final String DDB_CONTAINER_LEN = "DISTRIBUTEDDATABASE_CONTAINER_LEN";

  /**
   * Location of Script on all nodes
   */
  public static final String DB_SCRIPT_LOCATION = "exec_cmd.py";
  public static final String WRAP_SCRIPT_LOCATION = "exec_query.sh";
  //public static final String CONTAINER_SCRIPT_LOCATION = "exec_containor.py";
  
  /**
   * Port Number where App Master is listening 
   */
  /*public static final int CLIENT_PORT_NO  = 54000;
  public static final int APP_MASTER_PORT = 55000;*/
  
  /**
   * Command Line Constants 
   */
  
  public static final String CLIENT_HOST_NAME = "clientHostNameArg";
  public static final String CLIENT_PORT_NO = "clientPortNoArg";
  
  /**
   * Special Message Types
   */
  public static final String APP_MASTER_INFO = "APP_MASTER_INFO";
  
}

