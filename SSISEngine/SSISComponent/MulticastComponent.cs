using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime;
using System.Linq;

namespace SSISEngine
{
    public static class MulticastComponent
    {

        public static IDTSComponentMetaData100 CreateMulticast(MainPipe dft, string multicastName, out CManagedComponentWrapper WrapperMulticast, IDTSOutput100 iDTSOutput100)
        {

            IDTSComponentMetaData100 MetaDataMulticast = dft.ComponentMetaDataCollection.New();
            MetaDataMulticast.ComponentClassID = Constants.DTSTransformMulticast;
            WrapperMulticast = MetaDataMulticast.Instantiate();
            WrapperMulticast.ProvideComponentProperties();
          
            MetaDataMulticast.InputCollection[0].ExternalMetadataColumnCollection.IsUsed = false;
            MetaDataMulticast.InputCollection[0].HasSideEffects = false;
           
            //create path from source1 to merge
            IDTSPath100 pathSRC1merge = dft.PathCollection.New();
            pathSRC1merge.AttachPathAndPropagateNotifications(iDTSOutput100, MetaDataMulticast.InputCollection[0]);
            WrapperMulticast.AcquireConnections(null);
            WrapperMulticast.ReinitializeMetaData();
            WrapperMulticast.ReleaseConnections();
            MetaDataMulticast.Name = multicastName;

            return MetaDataMulticast;
        }
    }
}
