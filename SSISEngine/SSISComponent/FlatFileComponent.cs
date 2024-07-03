using System;
using System.IO;
using System.Text;
using System.Collections.Generic;
using System.Configuration;
using Microsoft.SqlServer.Dts.Runtime;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Wrapper = Microsoft.SqlServer.Dts.Runtime.Wrapper;
using System.Diagnostics;

namespace SSISEngine
{
    public static class FlatFileComponent
    {
        private static ConnectionManager flatFileConnectionManager;
        private static List<string> srcColumns = new List<string>();

        #region Adding Flat File Connection Manager to the Package

        /// <summary>
        /// 
        /// </summary>
        /// <param name="package">Package object </param>
        /// <param name="dataFlowTask">MainPipe object</param>
        /// <param name="app">Application object</param>
        /// <param name="flatFileSourceComponent">object of IDTSComponentMetaData100</param>
        /// <param name="connMgrFlatFileName">Provide the Flat file connection Name should be Unique for each connection</param>
        /// <param name="FlatFileComponentName">Provide the Flat file Name should be Unique for each Source and desination</param>
        /// <param name="filepath">Provide the location of the CSV flat file</param>
        public static IDTSComponentMetaData100 CreateFlatFile(Package package, MainPipe dataFlowTask, Application app, string connMgrFlatFileName, string FlatFileComponentName, string filepath)
        {
            IDTSComponentMetaData100 flatFileComponent;
            CManagedComponentWrapper flatFileInstance;
            ConnectionManager connMgrFlatFile = createFLATFile(package, connMgrFlatFileName, filepath);
           
            // Add a Flat File Source Component to the Data Flow Task
            flatFileComponent = dataFlowTask.ComponentMetaDataCollection.New();           
            flatFileComponent.ComponentClassID = app.PipelineComponentInfos["Flat File Source"].CreationName;

           

            // Get the design time instance of the Flat File Source Component
            flatFileInstance = flatFileComponent.Instantiate();
            flatFileInstance.ProvideComponentProperties();
            flatFileComponent.RuntimeConnectionCollection[0].ConnectionManager = DtsConvert.GetExtendedInterface(connMgrFlatFile);
            flatFileComponent.RuntimeConnectionCollection[0].ConnectionManagerID = connMgrFlatFile.ID;

            // Reinitialize the metadata.
            flatFileInstance.AcquireConnections(null);
            flatFileInstance.ReinitializeMetaData();
            flatFileInstance.ReleaseConnections();
            flatFileComponent.Name = FlatFileComponentName;
            return flatFileComponent;
        }

        #endregion

        #region Connection Manager
        /// <summary>
        /// This Method is used to connect the flat file in the SSIS Package
        /// </summary>
        /// <param name="file"></param>
        /// <param name="package"></param>
        /// <param name="connMgrFlatFileName"></param>
        /// <returns></returns>
        private static ConnectionManager createFLATFile_old(Package package,  string connMgrFlatFileName,string filepath)
        {
            ConnectionManager connMgrFlatFile = package.Connections.Add("FLATFILE");
            string[] columns = null;
            connMgrFlatFile.ConnectionString = filepath;
            connMgrFlatFile.Name = connMgrFlatFileName;//"Synkrino Flat File Connection";
            connMgrFlatFile.Description = "Flat File Connection";
            connMgrFlatFile.Properties["ColumnNamesInFirstDataRow"].SetValue(connMgrFlatFile, true);
            connMgrFlatFile.Properties["HeaderRowDelimiter"].SetValue(connMgrFlatFile, "{CR}{LF}");

            // Get the Column names to be used in configuring the Flat File Connection 
            // by reading the first line of the Import File which contains the Field names
            using (var stream = new StreamReader(filepath))
            {
                var fieldNames = stream.ReadLine();
                if (fieldNames != null) columns = fieldNames.Split(",".ToCharArray());
            }

            // Configure Columns and their Properties for the Flat File Connection Manager
            var connMgrFlatFileInnerObj = (Wrapper.IDTSConnectionManagerFlatFile100)connMgrFlatFile.InnerObject;

            connMgrFlatFileInnerObj.RowDelimiter = "{CR}{LF}";
            connMgrFlatFileInnerObj.ColumnNamesInFirstDataRow = true;

            if (columns != null)
            {
                foreach (var column in columns)
                {
                    // Add a new Column to the Flat File Connection Manager
                    var flatFileColumn = connMgrFlatFileInnerObj.Columns.Add();

                    flatFileColumn.DataType = Wrapper.DataType.DT_WSTR;
                    flatFileColumn.ColumnWidth = 255;
                    flatFileColumn.ColumnDelimiter = columns.GetUpperBound(0) == Array.IndexOf(columns, column) ? "{CR}{LF}" : ",";
                    flatFileColumn.ColumnType = "Delimited";
                    // Use the Import File Field name to name the Column
                    var columnName = flatFileColumn as Wrapper.IDTSName100;
                    if (columnName != null) columnName.Name = column;
                }
            }
            flatFileConnectionManager = connMgrFlatFile;


            return connMgrFlatFile;
        }

        private static ConnectionManager createFLATFile(Package package, string connMgrFlatFileName, string filepath)
        {
            ConnectionManager connMgrFlatFile = package.Connections.Add("FLATFILE");
            string[] columns = null;

            connMgrFlatFile.ConnectionString = filepath;
            connMgrFlatFile.Name = connMgrFlatFileName;//"Synkrino Flat File Connection";
            connMgrFlatFile.Description = "Description " + connMgrFlatFileName;
            connMgrFlatFile.Properties["ColumnNamesInFirstDataRow"].SetValue(connMgrFlatFile, true);
            connMgrFlatFile.Properties["HeaderRowDelimiter"].SetValue(connMgrFlatFile, "{CR}{LF}");
            connMgrFlatFile.Properties["TextQualifier"].SetValue(connMgrFlatFile, @"""");

            // Get the Column names to be used in configuring the Flat File Connection 
            // by reading the first line of the Import File which contains the Field names
            using (var stream = new StreamReader(filepath))
            {
                var fieldNames = stream.ReadLine();
                if (fieldNames != null) columns = fieldNames.Split(",".ToCharArray());
            }

            // Configure Columns and their Properties for the Flat File Connection Manager
            var connMgrFlatFileInnerObj = (Wrapper.IDTSConnectionManagerFlatFile100)connMgrFlatFile.InnerObject;

            connMgrFlatFileInnerObj.RowDelimiter = "{CR}{LF}";
            connMgrFlatFileInnerObj.ColumnNamesInFirstDataRow = true;
            connMgrFlatFileInnerObj.HeaderRowsToSkip = 0;
            string ext = Path.GetExtension(filepath);
            
            if (columns != null)
            {
                foreach (var column in columns)
                {
                    // Add a new Column to the Flat File Connection Manager
                    var flatFileColumn = connMgrFlatFileInnerObj.Columns.Add();

                    flatFileColumn.DataType = Wrapper.DataType.DT_WSTR;
                    flatFileColumn.ColumnWidth = 255;
                    flatFileColumn.TextQualified = true;
                    flatFileColumn.MaximumWidth = 255;
                    flatFileColumn.DataPrecision = 0;
                    flatFileColumn.DataScale = 0;
                    flatFileColumn.ColumnDelimiter = columns.GetUpperBound(0) == Array.IndexOf(columns, column) ? "\r\n" : ",";
                    flatFileColumn.ColumnType = "Delimited";
                    // Use the Import File Field name to name the Column
                    var columnName = flatFileColumn as Wrapper.IDTSName100;
                    if (columnName != null) columnName.Name = column;
                }
            }
            // flatFileConnectionManager = connMgrFlatFile;


            return connMgrFlatFile;
        }

        #endregion
    }
}
