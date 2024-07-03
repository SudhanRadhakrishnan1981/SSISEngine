using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime;

namespace SSISEngine
{
    public static class SortDTSTransform
    {
        #region Sort transform
        public static IDTSComponentMetaData100 CreateDTSTransformSort(Package package, MainPipe dataFlowTask, Application app, IDTSComponentMetaData100 srcComponent, ConnectionManager connMgrFlatFile,string sortName)
        {

            // Add transform
            IDTSComponentMetaData100 SortComponent = dataFlowTask.ComponentMetaDataCollection.New();
            SortComponent.ComponentClassID = Constants.DTSTransformSort;
            SortComponent.Name = sortName;
            CManagedComponentWrapper SortWrapper = SortComponent.Instantiate();

            SortWrapper.ProvideComponentProperties();


            // initialize metadata
            SortWrapper.AcquireConnections(null);
            SortWrapper.ReinitializeMetaData();
            SortWrapper.ReleaseConnections();

            // Connect the source and the transform
            IDTSPath100 SortPath = dataFlowTask.PathCollection.New();
            SortPath.AttachPathAndPropagateNotifications(srcComponent.OutputCollection[0], SortComponent.InputCollection[0]);

            // Get the list of available columns
            var SortComponentInput = SortComponent.InputCollection[0];
            // Get the Virtual Input Column Collection
            var SortvInput = SortComponentInput.GetVirtualInput();
            var SortVirtualInputColumns = SortvInput.VirtualInputColumnCollection;

            //Map the columns on the Mapping pages
            // Map sort File Source Component Output Columns to Ole Db Destination Input Columns
            foreach (IDTSVirtualInputColumn100 vColumn in SortVirtualInputColumns)
            {
                var inputColumn = SortWrapper.SetUsageType(SortComponentInput.ID,
                    SortvInput, vColumn.LineageID, DTSUsageType.UT_READONLY);
                //var externalColumn = SortComponentInput.InputColumnCollection[inputColumn.Name];
                SortWrapper.MapInputColumn(SortComponentInput.ID, inputColumn.ID, 0);
            }



            return SortComponent;
        }
        #endregion
    }
}
