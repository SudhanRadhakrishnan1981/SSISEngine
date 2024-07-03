using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using System;
namespace SSISEngine
{
    public static class MergeJoinComponent
    {

        public static IDTSComponentMetaData100 MergeJoin(MainPipe dft, string mergeJoinComponentName, IDTSComponentMetaData100 left, IDTSComponentMetaData100 right)
        {

            IDTSComponentMetaData100 merge = dft.ComponentMetaDataCollection.New();
            merge.ComponentClassID = Constants.DTSTransformMergeJoin;
            CManagedComponentWrapper mergeDesigntime = merge.Instantiate();
            mergeDesigntime.ProvideComponentProperties();
          

           // Console.WriteLine("merge created");
            merge.InputCollection[0].ExternalMetadataColumnCollection.IsUsed = false;
            merge.InputCollection[0].HasSideEffects = false;
            merge.InputCollection[1].ExternalMetadataColumnCollection.IsUsed = false;
            merge.InputCollection[1].HasSideEffects = false;

            #region  sort the both input ComponentMetaData         
            //left.OutputCollection[0].IsSorted = true;
            //left.OutputCollection[0].OutputColumnCollection[0].SortKeyPosition = 1;
            //right.OutputCollection[0].IsSorted = true;
            //right.OutputCollection[0].OutputColumnCollection[0].SortKeyPosition = 1;

            #endregion

            
            //create path from source1 to merge

            IDTSPath100 pathSRC1merge = dft.PathCollection.New();
            pathSRC1merge.AttachPathAndPropagateNotifications(left.OutputCollection[0], merge.InputCollection[0]);

            //create path from source2 to merge HookupRowCountTransform
            
            IDTSPath100 pathSrc2merge = dft.PathCollection.New();
            pathSrc2merge.AttachPathAndPropagateNotifications(right.OutputCollection[0], merge.InputCollection[1]);

            int firstrowcount = 0;
            int NumKeyColumns = 0;
            int NumKeyColumnssec = 0;
            IDTSInput100 mergeInput1 = merge.InputCollection[0];

            IDTSVirtualInput100 vMergeInput1 = mergeInput1.GetVirtualInput();
            foreach (IDTSVirtualInputColumn100 vColumn in vMergeInput1.VirtualInputColumnCollection)
            {
                mergeDesigntime.SetUsageType(mergeInput1.ID, vMergeInput1, vColumn.LineageID, DTSUsageType.UT_READONLY);
                if (vColumn.SortKeyPosition > NumKeyColumns)
                    NumKeyColumns = vColumn.SortKeyPosition;
                firstrowcount++;
            }
            
            IDTSInput100 mergeInput2 = merge.InputCollection[1];
            IDTSVirtualInput100 vMergeInput2 = mergeInput2.GetVirtualInput();
            foreach (IDTSVirtualInputColumn100 vColumn in vMergeInput2.VirtualInputColumnCollection)
            {
                mergeDesigntime.SetUsageType(mergeInput2.ID, vMergeInput2, vColumn.LineageID, DTSUsageType.UT_READONLY);
                if (vColumn.SortKeyPosition > NumKeyColumnssec)
                    NumKeyColumnssec = vColumn.SortKeyPosition;
            }
            //Must code to open the compn
            IDTSCustomProperty100 property1 = merge.CustomPropertyCollection[0];
            property1.Value = 2;
            IDTSCustomProperty100 property2 = merge.CustomPropertyCollection[1];
            property2.Value = 1;

            IDTSOutput100 outt = merge.OutputCollection[0];
            outt.IsSorted = true;

            //Below lines is to merge join the both columns must code.
            for (int i = firstrowcount; i < outt.OutputColumnCollection.Count; i++)
            {
              //  Console.WriteLine("out column: {0}", outt.OutputColumnCollection[i].Name);
                if (!outt.OutputColumnCollection[i].Name.Contains("DST_"))
                {
                    outt.OutputColumnCollection[i].Name = "DST_" + outt.OutputColumnCollection[i].Name;
                }
               
            }

            //Specify the join type below 2- inner join; 1-left outer; 0 -Full outer join.
            mergeDesigntime.SetComponentProperty("JoinType", 0); //left outer 

            mergeDesigntime.AcquireConnections(null);
            mergeDesigntime.ReinitializeMetaData();
            mergeDesigntime.ReleaseConnections();
            merge.Name = mergeJoinComponentName;
            return merge;
        }
    }
}
