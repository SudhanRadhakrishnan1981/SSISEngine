using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime;
using System.Linq;

namespace SSISEngine
{
    public static class ConditionalSplitComponent
    {
        public static IDTSComponentMetaData100 ConditionalSplit(MainPipe dataFlowTask, IDTSOutput100 dTSOutput100, string conditionalSplitName,string[] columnMaping)
        {
            // Add an ConditionalSplit to the data flow.
            IDTSComponentMetaData100 conditionalSplit = dataFlowTask.ComponentMetaDataCollection.New();
          
            conditionalSplit.ComponentClassID = Constants.DTSConditionalSplit;
            CManagedComponentWrapper conditionalSplitDesignTime = conditionalSplit.Instantiate();
            conditionalSplitDesignTime.ProvideComponentProperties();
            conditionalSplit.InputCollection[0].ExternalMetadataColumnCollection.IsUsed = false;
            conditionalSplit.InputCollection[0].HasSideEffects = false;
            conditionalSplitDesignTime.AcquireConnections(null);
            conditionalSplitDesignTime.ReinitializeMetaData();
            conditionalSplitDesignTime.ReleaseConnections();

            // Create the path from Source to Split Column Transformation.
            IDTSPath100 pathSourceconditionalSplit = dataFlowTask.PathCollection.New();
            pathSourceconditionalSplit.AttachPathAndPropagateNotifications(dTSOutput100,
                conditionalSplit.InputCollection[0]);

            IDTSInput100 conditionalSplitinput = conditionalSplit.InputCollection[0];
            // Set Usagetype for Input column, so that they are available for coding
            IDTSVirtualInput100 vdestinationinput  = conditionalSplitinput.GetVirtualInput();

            foreach (string mapcolumn in columnMaping)
            {
                IDTSVirtualInputColumn100 vColumnMap = vdestinationinput.VirtualInputColumnCollection.Cast<IDTSVirtualInputColumn100>().SingleOrDefault(a => a.Name == mapcolumn);
                if (vColumnMap != null)
                {
                    conditionalSplitDesignTime.SetUsageType(conditionalSplit.InputCollection[0].ID, vdestinationinput, vColumnMap.LineageID, DTSUsageType.UT_READONLY);
                }
            }

            // This method need to be developed as per the business requirement.
            conditionalSplit.Name = conditionalSplitName;
          
           
            return conditionalSplit;
        }

        public static void ConditionalSplitFilerCondtion(IDTSComponentMetaData100 conditionalSplit,string conditionalName,string expressionValue,string evaluationOrder)
        {
            var newOutputCollection = conditionalSplit.OutputCollection.New();
         
            newOutputCollection.HasSideEffects = false;
            newOutputCollection.ExclusionGroup = 1;
            newOutputCollection.ExternalMetadataColumnCollection.IsUsed = false;
            newOutputCollection.ErrorRowDisposition = DTSRowDisposition.RD_FailComponent;
            newOutputCollection.TruncationRowDisposition = DTSRowDisposition.RD_FailComponent;
            newOutputCollection.ErrorOrTruncationOperation = "Computation";
            newOutputCollection.SynchronousInputID = conditionalSplit.InputCollection[0].ID;
            IDTSCustomProperty100 myPropCS = newOutputCollection.CustomPropertyCollection.New();
            myPropCS.ContainsID = true;
            myPropCS.Name = "Expression";
            myPropCS.Value = expressionValue;//Age will be the column name is the connected Table
            myPropCS.ExpressionType = DTSCustomPropertyExpressionType.CPET_NOTIFY;
            myPropCS = newOutputCollection.CustomPropertyCollection.New();
            myPropCS.ContainsID = true;
            myPropCS.Name = "FriendlyExpression";
            myPropCS.Value = expressionValue;
            myPropCS.ExpressionType = DTSCustomPropertyExpressionType.CPET_NOTIFY;
            myPropCS = newOutputCollection.CustomPropertyCollection.New();
            myPropCS.ContainsID = true;
            myPropCS.Name = "EvaluationOrder";
            myPropCS.Value = evaluationOrder;
            myPropCS.ExpressionType = DTSCustomPropertyExpressionType.CPET_NOTIFY;

            newOutputCollection.Name = conditionalName;

        }

    }
}
