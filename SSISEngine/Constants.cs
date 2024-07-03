using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SSISEngine
{
    public static class Constants
    {
        public const string ConnectionString = "SynkrinoConnection";
        public const string LocalConnectionString = "LocalConnection";
        public const string SSISConnectionString = "SSISConnection";
        public const string ProjectName = "SSISSynkrinoDemoProject";
        public const string FolderName = "SynkrinoDemo";
        public const string ExcelConnectionString = @"Provider=Microsoft.ACE.OLEDB.12.0;Data Source = {0};Extended Properties = ""EXCEL 12.0;HDR=YES""";

        public const string DTSOleDbSource = "DTSAdapter.OleDbSource";
        public const string DTSOleDbDestination = "DTSAdapter.OleDbDestination";
        public const string DTSAdapterExcelDestination = "DTSAdapter.ExcelDestination";
        public const string DTSConditionalSplit = "DTSTransform.ConditionalSplit";
        public const string DTSAdapterOleDbSource = "DTSAdapter.OleDbSource";
        public const string DTSAdapterExcelSource = "DTSAdapter.ExcelSource";//,
        public const string DTSTransformRowCount = "DTSTransform.RowCount";
        public const string DTSTransformDerivedColumn = "DTSTransform.DerivedColumn";
        public const string DTSTransformOLEDBCommand = "DTSTransform.OLEDBCommand";
        public const string DTSTransformMulticast = "DTSTransform.Multicast";

        public const string DTSTransformLookup = "DTSTransform.Lookup";
        public const string DTSTransformMergeJoin = "DTSTransform.MergeJoin";//
        public const string DTSTransformSort = "DTSTransform.Sort";
        public const string Condiexpression = " != ";
        public const string CondiEqual = " == ";
        public const string columnDelimiter = "|";
        public const string Destination = "DST_";

        /*
         * 
         ComponentType||CreationName||ID||FileName
DestinationAdapter||DTSAdapter.RawDestination.3
Transform||DTSTransform.PctSampling.3
Transform||DTSTransform.TermExtraction.3
DestinationAdapter||AttunitySSISODBCDst.2
Transform||DTSTransform.MergeJoin.3
SourceAdapter||DTSAdapter.OLEDBSource.3
DestinationAdapter||DTSAdapter.ExcelDestination.3
Transform||DTSTransform.SCD.3
Transform||DTSTransform.RowSampling.3
DestinationAdapter||MSMDPP.PXPipelineProcessDimension.3
Transform||DTSTransform.Merge.3
DestinationAdapter||MSMDPP.PXPipelineProcessDM.3
SourceAdapter||DTSAdapter.RawSource.3
Transform||DTSTransform.DerivedColumn.3
DestinationAdapter||DTSAdapter.OLEDBDestination.3
Transform||DTSTransform.Lineage.3
Transform||DTSTransform.Sort.3
Transform||DTSTransform.Aggregate.3
Transform||DTSTransform.DataConvert.3
Transform||DTSTransform.Lookup.3
Transform||DTSTransform.ConditionalSplit.3
Transform||DTSTransform.GroupDups.3
SourceAdapter||DTSAdapter.ExcelSource.3
DestinationAdapter||DTSAdapter.FlatFileDestination.3
Transform||TxFileInserter.Inserter.3
Transform||DTSTransform.OLEDBCommand.3
SourceAdapter||AttunitySSISODBCSrc.2
Transform||TxFileExtractor.Extractor.3
Transform||DTSTransform.TermLookup.3
Transform||DTSTransform.Pivot.3
Transform||DTSTransform.UnionAll.3
Transform||DTSTransform.UnPivot.3
Transform||DTSTransform.Cache.2
DestinationAdapter||DTSAdapter.RecordsetDestination.3
Transform||DTSTransform.CharacterMap.3
Transform||MSMDPP.PXPipelineDMQuery.3
Transform||DTSTransform.CopyMap.3
SourceAdapter||DTSAdapter.FlatFileSource.3
DestinationAdapter||MSMDPP.PXPipelineProcessPartition.3
Transform||DTSTransform.RowCount.3
Transform||DTSTransform.BestMatch.3
Transform||DTSTransform.Multicast.3
Transform||DTSTransform.BalancedDataDistributor.2
DestinationAdapter||DTSAdapter.SQLServerDestination.3
             
             
             
             */


        public const string AccessMode = "AccessMode";
        /// <summary>
        /// AccessMode will be 0
        /// </summary>
        public const string OpenRowset = "OpenRowset";
        /// <summary>
        /// SqlCommand will be 2
        /// </summary>
        public const string SqlCommand = "SqlCommand";
        /// <summary>
        /// CommandTimeout will be 0
        /// </summary>
        public const string CommandTimeout = "CommandTimeout";


        public const string CONN_STRING_OUTPUT_DB = "CONN_STRING_OUTPUT_DB";
        public const string FILE_PATH_DST_EMP_ADDRESS = "FILE_PATH_DST_EMP_ADDRESS";
        public const string FILE_PATH_DST_EMPLOYEE = "FILE_PATH_DST_EMPLOYEE";
        public const string FILE_PATH_SRC_EMP_ADDRESS = "FILE_PATH_SRC_EMP_ADDRESS";
        public const string FILE_PATH_SRC_EMPLOYEE = "FILE_PATH_SRC_EMPLOYEE";
        public const string VAR_EMP_ADDRESS_COUNT = "VAR_EMP_ADDRESS_COUNT";
        public const string VAR_EMPLOYEE_COUNT = "VAR_EMPLOYEE_COUNT";
        public const string VAR_TOTAL_COUNT = "VAR_TOTAL_COUNT";
        public const string VAR_EMP_DETAILS_COUNT = "VAR_EMP_DETAILS_COUNT";

    }
    public enum Operator
    {
        SQLCOMMAND,OPENROWSET,SQLSP
    }
}
