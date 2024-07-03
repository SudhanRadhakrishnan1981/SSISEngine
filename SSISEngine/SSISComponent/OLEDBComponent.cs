using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime;
using System.Linq;
namespace SSISEngine
{
    public static class OLEDBComponent
    {
       // private static readonly string _ExcelconnectionString = ConfigurationManager.ConnectionStrings[Constants.ExcelConnectionString].ConnectionString;
        public static IDTSComponentMetaData100 CreateOLEDBCommand(Package package, MainPipe dataFlowTask, Application app, out CManagedComponentWrapper oleDbInstance, string OLEDBDestinationName, string SourceSQL, string OledbConnectionName, Operator op, string OLEDBType, IDTSOutput100 iDTSOutput100,string mapingString, ConnectionManager databaseConnectionManager)
        {
            IDTSComponentMetaData100 oleDbComponent;
            // Add a Connection Manager to the Package, of type, OLEDB            
            // ConnectionManager connMgrOleDb = SetConnectionManager.CreateOLEDBConnectionManager(package, OledbConnectionName);
            ConnectionManager connMgrOleDb = databaseConnectionManager;
            // Add an OLE DB Destination Component to the Data Flow
            oleDbComponent = dataFlowTask.ComponentMetaDataCollection.New();

            oleDbComponent.ComponentClassID = OLEDBType;
            // Get the design time instance of the Ole Db Destination componentsss
            oleDbInstance = oleDbComponent.Instantiate();
            oleDbInstance.ProvideComponentProperties();

            // Set Ole Db Destination Connection
            oleDbComponent.RuntimeConnectionCollection[0].ConnectionManagerID = connMgrOleDb.ID;
            oleDbComponent.RuntimeConnectionCollection[0].ConnectionManager =
                DtsConvert.GetExtendedInterface(connMgrOleDb);
            switch (op)
            {
                case Operator.SQLSP:
                    oleDbInstance.SetComponentProperty(Constants.CommandTimeout, 0);
                    oleDbInstance.SetComponentProperty(Constants.SqlCommand, SourceSQL);
                    break;
            }

            // Set the Access Mode and SQL Command to the OleDB Source            

            // Reinitialize the metadata
            oleDbInstance.AcquireConnections(null);
            oleDbInstance.ReinitializeMetaData();
            oleDbInstance.ReleaseConnections();

            // Create the path from source to destination
            IDTSPath100 pathDestination = dataFlowTask.PathCollection.New();
            pathDestination.AttachPathAndPropagateNotifications(iDTSOutput100,
                oleDbComponent.InputCollection[0]);
            ColumnMapping(oleDbInstance, mapingString, oleDbComponent);

            oleDbComponent.Name = OLEDBDestinationName;

            return oleDbComponent;
        }

        private static void ColumnMapping(CManagedComponentWrapper oleDbInstance, string mapingString, IDTSComponentMetaData100 oleDbComponent)
        {
            // Get the destination's default input and virtual input.
            IDTSInput100 destinationinput = oleDbComponent.InputCollection[0];
            //int destinationInputID = destinationinput.ID;

            IDTSVirtualInput100 vdestinationinput = destinationinput.GetVirtualInput();


            // Iterate through the virtual input column collection.
            foreach (IDTSVirtualInputColumn100 vColumn in
                vdestinationinput.VirtualInputColumnCollection)
            {
                    string cinputColumnName = string.Empty;
                    cinputColumnName = stringHandling(mapingString, vColumn, cinputColumnName);
                    var columnExist = (from item in destinationinput.ExternalMetadataColumnCollection.Cast<IDTSExternalMetadataColumn100>()
                                       where item.Name == "@" + cinputColumnName
                                       select item).Count();
                    if (columnExist > 0)
                    {
                        IDTSInputColumn100 vCol = oleDbInstance.SetUsageType(destinationinput.ID,
                                                    vdestinationinput, vColumn.LineageID, DTSUsageType.UT_READWRITE);
                        oleDbInstance.MapInputColumn(destinationinput.ID, vCol.ID,
                                                    destinationinput.ExternalMetadataColumnCollection["@" + cinputColumnName].ID);

                    }

            }
        }

        private static string stringHandling(string mapingString, IDTSVirtualInputColumn100 vColumn, string cinputColumnName)
        {

            if (mapingString.Contains('|'))
            {
                string[] array = mapingString.Split('|').ToArray();
                foreach (string pair in array)
                {
                    if (vColumn.Name == pair)
                    {
                        if (vColumn.Name == array[1])
                            cinputColumnName = "SRC_COLUMN_VALUE";
                        else if (vColumn.Name == array[2])
                            cinputColumnName = "DST_COLUMN_VALUE";
                    }
                    else if (vColumn.Name.Contains(pair) && (vColumn.Name.Contains("DST_") || vColumn.Name.Contains("SRC_")))
                    {
                        cinputColumnName = vColumn.Name.Remove(vColumn.Name.IndexOf(pair), pair.Length);
                    }
                }
            }
            else
            {
                if (vColumn.Name.Contains(mapingString))
                {
                    cinputColumnName = vColumn.Name.Remove(vColumn.Name.IndexOf(mapingString), mapingString.Length);
                }

            }
               
                  

            return cinputColumnName;
        }

        public static void ColumnMapping(CManagedComponentWrapper oleDbInstance, string[] mapingStrings, IDTSComponentMetaData100 oleDbComponent)
        {
            // Get the destination's default input and virtual input.
            IDTSInput100 destinationinput = oleDbComponent.InputCollection[0];
            //int destinationInputID = destinationinput.ID;

            IDTSVirtualInput100 vdestinationinput = destinationinput.GetVirtualInput();


            // Iterate through the virtual input column collection.
            foreach (IDTSVirtualInputColumn100 vColumn in
                vdestinationinput.VirtualInputColumnCollection)
            {
                string cinputColumnName = string.Empty;
                string mapingString = mapingStrings[0];
                if (vColumn.Name.Contains(mapingString))
                {
                    cinputColumnName = vColumn.Name.Remove(vColumn.Name.IndexOf(mapingString), mapingString.Length);
                }
                var columnExist = (from item in destinationinput.ExternalMetadataColumnCollection.Cast<IDTSExternalMetadataColumn100>()
                                   where item.Name == "@" + cinputColumnName
                                   select item).Count();
                if (columnExist > 0)
                {
                    IDTSInputColumn100 vCol = oleDbInstance.SetUsageType(destinationinput.ID,
                                                vdestinationinput, vColumn.LineageID, DTSUsageType.UT_READWRITE);
                    oleDbInstance.MapInputColumn(destinationinput.ID, vCol.ID,
                                                destinationinput.ExternalMetadataColumnCollection["@" + cinputColumnName].ID);

                }

            }
        }

        public static IDTSComponentMetaData100 CreateOLEDBComponent(Package package, MainPipe dataFlowTask, Application app, out CManagedComponentWrapper oleDbInstance, string OLEDBDestinationName, string TableName, string OledbConnectionName, Operator op, string OLEDBType, IDTSComponentMetaData100 source, int index)
        {
            IDTSComponentMetaData100 oleDbComponent;
            // Add a Connection Manager to the Package, of type, OLEDB            
            ConnectionManager connMgrOleDb = SetConnectionManager.CreateOLEDBConnectionManager(package, OledbConnectionName);
            // Add an OLE DB Destination Component to the Data Flow
            oleDbComponent = dataFlowTask.ComponentMetaDataCollection.New();

            oleDbComponent.ComponentClassID = OLEDBType;
            // Get the design time instance of the Ole Db Destination componentsss
            oleDbInstance = oleDbComponent.Instantiate();
            oleDbInstance.ProvideComponentProperties();

            // Set Ole Db Destination Connection
            oleDbComponent.RuntimeConnectionCollection[0].ConnectionManagerID = connMgrOleDb.ID;
            oleDbComponent.RuntimeConnectionCollection[0].ConnectionManager =
                DtsConvert.GetExtendedInterface(connMgrOleDb);
            //TODO make this Query as dynamic
            string SourceSQL = TableName;//"Select * from Orders";
            switch (op)
            {
                case Operator.OPENROWSET:
                    // Set destination load type
                    oleDbInstance.SetComponentProperty(Constants.AccessMode, 3);
                    // Now set Ole Db Destination Table name
                    oleDbInstance.SetComponentProperty(Constants.OpenRowset, TableName);
                    break;
                case Operator.SQLCOMMAND:
                    oleDbInstance.SetComponentProperty(Constants.AccessMode, 2);
                    oleDbInstance.SetComponentProperty(Constants.SqlCommand, SourceSQL);
                    break;
                case Operator.SQLSP:
                    oleDbInstance.SetComponentProperty(Constants.CommandTimeout, 0);
                    oleDbInstance.SetComponentProperty(Constants.SqlCommand, SourceSQL);
                    break;
            }

            // Set the Access Mode and SQL Command to the OleDB Source            

            // Reinitialize the metadata
            oleDbInstance.AcquireConnections(null);
            oleDbInstance.ReinitializeMetaData();
            oleDbInstance.ReleaseConnections();

            // Create the path from source to destination
            IDTSPath100 pathDestination = dataFlowTask.PathCollection.New();
            pathDestination.AttachPathAndPropagateNotifications(source.OutputCollection[index],
                oleDbComponent.InputCollection[0]);


            // Get the destination's default input and virtual input.
            IDTSInput100 destinationinput = oleDbComponent.InputCollection[0];
            //int destinationInputID = destinationinput.ID;

            IDTSVirtualInput100 vdestinationinput = destinationinput.GetVirtualInput();


            // Iterate through the virtual input column collection.
            foreach (IDTSVirtualInputColumn100 vColumn in
                vdestinationinput.VirtualInputColumnCollection)
            {
                IDTSInputColumn100 vCol = oleDbInstance.SetUsageType(destinationinput.ID,
                                                                      vdestinationinput, vColumn.LineageID, DTSUsageType.UT_READWRITE);

                string cinputColumnName = vColumn.Name;
                var columnExist = (from item in destinationinput.ExternalMetadataColumnCollection.Cast<IDTSExternalMetadataColumn100>()
                                   where item.Name == cinputColumnName
                                   select item).Count();
                if (columnExist > 0)
                    oleDbInstance.MapInputColumn(destinationinput.ID, vCol.ID,
                                                  destinationinput.ExternalMetadataColumnCollection[vColumn.Name].ID);
            }

            oleDbComponent.Name = OLEDBDestinationName;//MyOLEDBDestination

            return oleDbComponent;
        }



        public static IDTSComponentMetaData100 CreateExcelComponent(Package package, MainPipe dataFlowTask, Application app, out IDTSDesigntimeComponent100 instance, string OLEDBDestinationName, string TableName, string ExcelConnectionName, string OLEDBType,string Excelpath)
        {
            IDTSComponentMetaData100 ExcelSource = null;
            // Add a Connection Manager to the Package, of type, OLEDB            
            ConnectionManager connMgrOleDb = SetConnectionManager.CreateEXCELConnectionManager(package, ExcelConnectionName, Excelpath);
           

            //connMgrOleDb.ConnectionString = string.Format(Constants.ExcelConnectionString, Excelpath);

            // Add an OLE DB Destination Component to the Data Flow
            ExcelSource = dataFlowTask.ComponentMetaDataCollection.New();
            //ExcelSource.Name = OLEDBDestinationName;//MyOLEDBDestination
            ExcelSource.Description = OLEDBDestinationName;
            ExcelSource.ComponentClassID = OLEDBType;
           // ExcelSource.ValidateExternalMetadata = false;
            
            // Get the design time instance of the Ole Db Destination componentsss
            instance = ExcelSource.Instantiate();
            instance.ProvideComponentProperties();

            // Set Ole Db Destination Connection
            ExcelSource.RuntimeConnectionCollection[0].ConnectionManagerID = connMgrOleDb.ID;
            ExcelSource.RuntimeConnectionCollection[0].ConnectionManager = DtsConvert.GetExtendedInterface(connMgrOleDb);

            // Set destination load type
            instance.SetComponentProperty(Constants.AccessMode, 0);
            // Now set Ole Db Destination Table name
            instance.SetComponentProperty(Constants.OpenRowset, TableName);


            // Set the Access Mode and SQL Command to the OleDB Source            

            // Reinitialize the metadata
            instance.AcquireConnections(null);
            instance.ReinitializeMetaData();
            instance.ReleaseConnections();
            ExcelSource.Name = OLEDBDestinationName;
            return ExcelSource;
        }


        public static IDTSComponentMetaData100 GetRowcount(Package package, MainPipe dataFlowTask, Application app, IDTSComponentMetaData100 MetaDataobj,string getrowCounter)
        {
            IDTSComponentMetaData100 dataConvertComponent = dataFlowTask.ComponentMetaDataCollection.New();
            Variable Counter = package.Variables.Add("var_counter", false, "User", 0);
            dataConvertComponent.ComponentClassID = Constants.DTSTransformRowCount;        
          
            CManagedComponentWrapper dataConvertWrapper = dataConvertComponent.Instantiate();
            dataConvertWrapper.ProvideComponentProperties();
            dataConvertWrapper.SetComponentProperty("VariableName", "User::var_counter");
            // Connect the source and the transform
            dataFlowTask.PathCollection.New().AttachPathAndPropagateNotifications(MetaDataobj.OutputCollection[0], dataConvertComponent.InputCollection[0]);
            //TODO if need we need to use the User::var_counter to get the count of the records.
            //TaskHost th = exec as TaskHost;
            //th.Properties["Name"].SetValue(th, "insert row count");
            //th.Properties["Description"].SetValue(th, "insert row count");
            //th.SetExpression("SqlStatementSource", "\"insert into your_table_name values('\" +(DT_WSTR, 100) @[User::var_counter] +\"')\"");
            //th.SetExpression("Connection", "\"ConMgr\"");
            dataConvertComponent.Name = getrowCounter;
            dataConvertComponent.Description = "gets row counter";
            return dataConvertComponent;
        }

    }
}
