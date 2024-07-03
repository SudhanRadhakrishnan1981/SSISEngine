using Microsoft.SqlServer.Dts.Runtime;
using System.Configuration;
namespace SSISEngine
{
    public static class SetConnectionManager
    {

        private static readonly string _connectionString = ConfigurationManager.ConnectionStrings[Constants.LocalConnectionString].ConnectionString;
       // private static readonly string _ExcelconnectionString = ConfigurationManager.ConnectionStrings[Constants.ExcelConnectionString].ConnectionString;
        public static ConnectionManager CreateOLEDBConnectionManager(Package package, string OLEDBConnectionName)
        {
            // Add a Connection Manager to the Package, of type, OLEDB 
            var connMgrOleDb = package.Connections.Add("OLEDB");
            connMgrOleDb.ConnectionString = _connectionString.ToString();
            connMgrOleDb.Name = OLEDBConnectionName;
            connMgrOleDb.Description = "OLE DB connection";
            return connMgrOleDb;
        }

        public static ConnectionManager CreateEXCELConnectionManager(Package package, string ExcelConnectionName,string excelpath)
        {
            // Add a Connection Manager to the Package, of type, OLEDB 
            ConnectionManager connMgrExcel = package.Connections.Add("Excel");
            connMgrExcel.ConnectionString = string.Format(Constants.ExcelConnectionString, excelpath);
            connMgrExcel.Name = ExcelConnectionName;
            connMgrExcel.Description = "Source data in the DataFlow";
            connMgrExcel.DelayValidation = true;
            return connMgrExcel;
        }
    }
}
