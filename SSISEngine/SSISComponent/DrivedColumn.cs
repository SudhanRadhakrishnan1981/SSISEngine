using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SSISEngine
{
    public static class DrivedColumn
    {
        public static IDTSComponentMetaData100 CreateDrivedColumn(MainPipe pipeline, IDTSComponentMetaData100 srcComponent, out CManagedComponentWrapper derivedColumnInstance,string DerivedColumnName, List<string> columnMaping)
        {
            //Derived Column
            IDTSComponentMetaData100 derivedColumn = pipeline.ComponentMetaDataCollection.New();
           
            derivedColumn.ComponentClassID = Constants.DTSTransformDerivedColumn;
            derivedColumnInstance = derivedColumn.Instantiate();
            derivedColumnInstance.ProvideComponentProperties();        //design time
            derivedColumn.InputCollection[0].ExternalMetadataColumnCollection.IsUsed = false;
            derivedColumn.OutputCollection[0].TruncationRowDisposition = DTSRowDisposition.RD_NotUsed;
            derivedColumn.OutputCollection[0].ErrorRowDisposition = DTSRowDisposition.RD_NotUsed;
            derivedColumn.InputCollection[0].HasSideEffects = false;
            //update the metadata for the derived columns
            derivedColumnInstance.AcquireConnections(null);
            derivedColumnInstance.ReinitializeMetaData();
            derivedColumnInstance.ReleaseConnections();

            //derivedColumn.OutputCollection[0].IsSorted = true;
            //Create the path from source to derived columns 
            IDTSPath100 SourceToDerivedPath = pipeline.PathCollection.New();
            SourceToDerivedPath.AttachPathAndPropagateNotifications(srcComponent.OutputCollection[0], derivedColumn.InputCollection[0]);



            CManagedComponentWrapper managedOleInstance = derivedColumn.Instantiate();
            // Get the derived's default input and virtual input.
            IDTSInput100 input = derivedColumn.InputCollection[0];
            IDTSVirtualInput100 vInput = input.GetVirtualInput();
            Dictionary<string, int> lineAgeIDs
               = new Dictionary<string, int>();

            //// Iterate through the virtual input column collection
            //foreach (IDTSVirtualInputColumn100 vColumn in
            //    vInput.VirtualInputColumnCollection)
            //{
            //    managedOleInstance.SetUsageType(
            //    input.ID, vInput, vColumn.LineageID,
            //    DTSUsageType.UT_READONLY);
            //    lineAgeIDs[vColumn.Name] = vColumn.LineageID;
            //}

            foreach (string mapcolumn in columnMaping)
            {
                IDTSVirtualInputColumn100 vColumnMap = vInput.VirtualInputColumnCollection.Cast<IDTSVirtualInputColumn100>().SingleOrDefault(a => a.Name == mapcolumn);
                if (vColumnMap != null)
                {
                    managedOleInstance.SetUsageType(derivedColumn.InputCollection[0].ID, vInput, vColumnMap.LineageID, DTSUsageType.UT_READONLY);
                    lineAgeIDs[vColumnMap.Name] = vColumnMap.LineageID;
                }
            }

            // putting the truncation row disposition
            derivedColumn.OutputCollection[0].TruncationRowDisposition = DTSRowDisposition.RD_NotUsed;
            // putting the error row disposition
            derivedColumn.OutputCollection[0].ErrorRowDisposition = DTSRowDisposition.RD_NotUsed;
            // get the output column collection reference
            IDTSOutput100 output = derivedColumn.OutputCollection[0];
            derivedColumn.Name = DerivedColumnName;
            return derivedColumn;
        }


        public static void ImplementDerivedCoulmn(IDTSComponentMetaData100 derived, string DerivedCoulmnName1, string DerivedCoulmnName2, string DerivedCoulmnName3)
        {
            //ImplementingDerivedCoulmn(derived, DerivedCoulmnName1);
            //ImplementingDerivedCoulmn1(derived, DerivedCoulmnName2);
            //ImplementingDerivedCoulmn2(derived, DerivedCoulmnName3);
        }

        internal static void ImplementingDerivedCoulmn(IDTSComponentMetaData100 derived,string DerivedCoulmnName,string expressionValue)
        {
            // //Derived Column one
            IDTSOutputColumn100 myCol = derived.OutputCollection[0].OutputColumnCollection.New();
            myCol.Name = DerivedCoulmnName;
            myCol.MappedColumnID = 1;
            myCol.SetDataTypeProperties(Microsoft.SqlServer.Dts.Runtime.Wrapper.DataType.DT_WSTR, 580, 0, 0, 0);
            myCol.ExternalMetadataColumnID = 0;
            myCol.ErrorRowDisposition = DTSRowDisposition.RD_FailComponent;
            myCol.TruncationRowDisposition = DTSRowDisposition.RD_FailComponent;

            //var Expression = new StringBuilder();

            //Expression.Append("(DT_STR,50,1252)[EMP_ID] +");
            //Expression.Append(@"""-""");
            //Expression.Append("+ (DT_STR,50,1252)[COMPANY_ID]");

            IDTSCustomProperty100 myProp = myCol.CustomPropertyCollection.New();
            myProp.Name = "Expression";
            myProp.Value = expressionValue;
            myProp = myCol.CustomPropertyCollection.New();
            myProp.Name = "FriendlyExpression";
            myProp.Value = expressionValue;
        }

      

        private static void ImplementingDerivedCoulmn1(IDTSComponentMetaData100 derived, string DerivedCoulmnName)
        {
            // //Derived Column one
            IDTSOutputColumn100 myCol = derived.OutputCollection[0].OutputColumnCollection.New();
            myCol.Name = DerivedCoulmnName;
            myCol.MappedColumnID = 2;
            myCol.SetDataTypeProperties(Microsoft.SqlServer.Dts.Runtime.Wrapper.DataType.DT_I4, 0, 0, 0, 0);
            myCol.ExternalMetadataColumnID = 0;
            myCol.ErrorRowDisposition = DTSRowDisposition.RD_FailComponent;
            myCol.TruncationRowDisposition = DTSRowDisposition.RD_FailComponent;
            IDTSCustomProperty100 myProp = myCol.CustomPropertyCollection.New();
            myProp.Name = "Expression";
            myProp.Value = "@OUTPUT_ID";
            myProp = myCol.CustomPropertyCollection.New();
            myProp.Name = "FriendlyExpression";
            myProp.Value = "@OUTPUT_ID";
        }
       

        private static void ImplementingDerivedCoulmn2(IDTSComponentMetaData100 derived, string DerivedCoulmnName)
        {
            // //Derived Column one
            IDTSOutputColumn100 myCol = derived.OutputCollection[0].OutputColumnCollection.New();
            myCol.Name = DerivedCoulmnName;
            myCol.MappedColumnID = 3;
            myCol.SetDataTypeProperties(Microsoft.SqlServer.Dts.Runtime.Wrapper.DataType.DT_DATE, 0, 0, 0, 0);
            myCol.ExternalMetadataColumnID = 0;
            myCol.ErrorRowDisposition = DTSRowDisposition.RD_FailComponent;
            myCol.TruncationRowDisposition = DTSRowDisposition.RD_FailComponent;
            IDTSCustomProperty100 myProp = myCol.CustomPropertyCollection.New();
            myProp.Name = "Expression";
            myProp.Value = "@[LOAD_TS]";
            myProp = myCol.CustomPropertyCollection.New();
            myProp.Name = "FriendlyExpression";
            myProp.Value = "@[LOAD_TS]";
        }


        private static void GetExpressionString(ref string expression, ref string friendlyExpression, Microsoft.SqlServer.Dts.Runtime.Wrapper.DataType dataType, int length, int precision, int scale, Dictionary<string, int> lineAgeIDs, string columnName)
        {
            expression = string.Empty;
            friendlyExpression = string.Empty;
            // get the lineage id for the column name
            int lineageID = lineAgeIDs[columnName];

            switch (dataType)
            {
                case Microsoft.SqlServer.Dts.Runtime.Wrapper.DataType.DT_STR:
                    expression =
                        string.Format("#{0}", lineageID);
                    friendlyExpression = string.Format("{0}", columnName);
                    break;
                case Microsoft.SqlServer.Dts.Runtime.Wrapper.DataType.DT_I4:
                    // 4 byte signed integer
                    expression =
                        string.Format("[ISNULL](#{0}) || #{1} ==    \"\" ? NULL(DT_I4) : (DT_I4)#{2}",
                        lineageID, lineageID, lineageID);
                    friendlyExpression = string.Format("ISNULL([{0}]) || [{1}] ==      \"\" ? NULL(DT_I4) : (DT_I4)[{2}]",
                        columnName, columnName, columnName);
                    break;
                case Microsoft.SqlServer.Dts.Runtime.Wrapper.DataType.DT_DECIMAL:

                case Microsoft.SqlServer.Dts.Runtime.Wrapper.DataType.DT_NUMERIC:
                    // Should we handle precision here?
                    expression = string.Format("[ISNULL](#{0}) || #{1} == \"\" ? NULL(DT_NUMERIC,{2},{3}) : (DT_NUMERIC,{4},{5})#  { 6} ", lineageID, lineageID, precision, scale, precision, scale, lineageID);
                    friendlyExpression = string.Format("[ISNULL]([{0}]) || [{1}] ==   \"\" ? NULL(DT_NUMERIC,{2},{3}) : (DT_NUMERIC,{4},  { 5})[{6}]", columnName, columnName, precision, scale, precision, scale, columnName);
                    break;
                case Microsoft.SqlServer.Dts.Runtime.Wrapper.DataType.DT_DATE:
                    expression = string.Format("[ISNULL](#{0}) || #{1} == \"\" ? NULL(DT_DATE) : (DT_DATE)#{2}", lineageID, lineageID, lineageID);
                    friendlyExpression = string.Format("ISNULL([{0}]) || [{1}] == \"\" ? NULL(DT_DATE) : (DT_DATE)[{2}]", columnName, columnName, columnName);
                    break;
                case Microsoft.SqlServer.Dts.Runtime.Wrapper.DataType.DT_I8:
                    expression = string.Format("[ISNULL](#{0}) || #{1} == \"\" ? NULL(DT_I8) : (DT_I8)#{2}", lineageID, lineageID, lineageID);
                    friendlyExpression = string.Format("ISNULL([{0}]) || [{1}] == \"\" ? NULL(DT_I8) : (DT_I8)[{2}]", columnName, columnName, columnName);
                    break;
                case Microsoft.SqlServer.Dts.Runtime.Wrapper.DataType.DT_BOOL:
                    expression = string.Format("[ISNULL](#{0}) || #{1} ==\"\" ? NULL(DT_BOOL) : (DT_BOOL)#{2}", lineageID, lineageID, lineageID);
                    friendlyExpression = string.Format("ISNULL([{0}]) || [{1}] ==  \"\" ? NULL(DT_BOOL) : (DT_BOOL)[{2}]", columnName, columnName, columnName);
                    break;
                default:
                    expression = string.Format("#{0}", lineageID);
                    friendlyExpression =
                            string.Format("{0}", columnName);
                    break;
            }
        }

        /// <summary>
        /// This method is used to create Drived Column.
        /// </summary>
        /// <param name="PrimaryColumns"></param>
        /// <param name="dataFlowTask"></param>
        /// <param name="TragetMetaDataobj"></param>
        /// <param name="expressionValue"></param>
        /// <param name="DrivedColumnName"></param>
        /// <returns></returns>
        public static IDTSComponentMetaData100 CreateDrivedColumn(List<string> PrimaryColumns, MainPipe dataFlowTask, IDTSComponentMetaData100 TragetMetaDataobj, string expressionValue, string DrivedColumnName)
        {
            //Derived Column for sources file
            CManagedComponentWrapper sourceDerivedColumnsWrapper;
            IDTSComponentMetaData100 sourcederivedMetaDataobj = CreateDrivedColumn(dataFlowTask, TragetMetaDataobj, out sourceDerivedColumnsWrapper, DrivedColumnName, PrimaryColumns);


            ImplementingDerivedCoulmn(sourcederivedMetaDataobj, "PRIMARY_KEYS", expressionValue);
            expressionValue = "@[OUTPUT_ID]";
            ImplementingDerivedCoulmn(sourcederivedMetaDataobj, "OUTPUT_ID", expressionValue);
            expressionValue = "@[LOAD_TS]";
            ImplementingDerivedCoulmn(sourcederivedMetaDataobj, "LOAD_TS", expressionValue);
            return sourcederivedMetaDataobj;
        }
    }
}
