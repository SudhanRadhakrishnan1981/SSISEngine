using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime;
using System.Configuration;

namespace SSISEngine
{
    public static class lookuptransform
    {


        #region lookup transform
        public static void Createlookuptransform(Package package, MainPipe dataFlowTask, Application app, IDTSComponentMetaData100 srcComponent, out IDTSComponentMetaData100 lookupComponent, out IDTSOutput100 lookupNoMatchOutput, out IDTSOutput100 lookupMatchOutput,string oledbConnectionName,string lookupName)
        {
            ConnectionManager connection = SetConnectionManager.CreateOLEDBConnectionManager(package, oledbConnectionName);

            // Add transform
            lookupComponent = dataFlowTask.ComponentMetaDataCollection.New();
            lookupComponent.ComponentClassID = Constants.DTSTransformLookup;
            lookupComponent.Name = lookupName;
            CManagedComponentWrapper lookupWrapper = lookupComponent.Instantiate();
            lookupWrapper.ProvideComponentProperties();

            // Connect the source and the transform
            IDTSPath100 lookUpPath = dataFlowTask.PathCollection.New();
            lookUpPath.AttachPathAndPropagateNotifications(srcComponent.OutputCollection[0], lookupComponent.InputCollection[0]);
            // Set the connection manager
            lookupComponent.RuntimeConnectionCollection[0].ConnectionManager = DtsConvert.GetExtendedInterface(connection);
            lookupComponent.RuntimeConnectionCollection[0].ConnectionManagerID = connection.ID;

            string SourceSQL = "Select * from Orders";  //Need to change this table or query as per the requirement.
            // Cache Type - Full = 0, Partial = 1, None = 2
            lookupWrapper.SetComponentProperty("CacheType", 0);
            lookupWrapper.SetComponentProperty("NoMatchBehavior", 1);// 1= Redirect rows to No Match output
            lookupWrapper.SetComponentProperty("SqlCommand", SourceSQL);

            // initialize metadata
            lookupWrapper.AcquireConnections(null);
            lookupWrapper.ReinitializeMetaData();
            lookupWrapper.ReleaseConnections();

            // Mark the columns we are joining on
            IDTSInput100 lookupInput = lookupComponent.InputCollection[0];
            IDTSInputColumnCollection100 lookupInputColumns = lookupInput.InputColumnCollection;
            IDTSVirtualInput100 lookupVirtualInput = lookupInput.GetVirtualInput();
            IDTSVirtualInputColumnCollection100 lookupVirtualInputColumns = lookupVirtualInput.VirtualInputColumnCollection;

            // Note: join columns should be marked as READONLY
            var joinColumns = new string[] { "Order_No" };
            foreach (string columnName in joinColumns)
            {
                IDTSVirtualInputColumn100 virtualColumn = lookupVirtualInputColumns[columnName];
                IDTSInputColumn100 inputColumn = lookupWrapper.SetUsageType(lookupInput.ID, lookupVirtualInput, virtualColumn.LineageID, DTSUsageType.UT_READONLY);
                lookupWrapper.SetInputColumnProperty(lookupInput.ID, inputColumn.ID, "JoinToReferenceColumn", columnName);
            }

            // below two items is used to  Connect source and destination for Matching Outputs
            // First output is the Match output
            lookupMatchOutput = lookupComponent.OutputCollection[0];

            // Second output is the Un-Match output
            lookupNoMatchOutput = lookupComponent.OutputCollection[1];


        }
        #endregion lookup transform
    }
}

