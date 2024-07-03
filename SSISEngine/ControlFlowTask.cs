using SSISEngine.EventHandler;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime;
using System;
using System.Configuration;
using System.Globalization;
using System.IO;
using System.Text;
using System.Linq;
using System.Reflection;
using System.Collections.Generic;

namespace SSISEngine
{
    public class ControlFlowTask
    {
        private readonly string _connectionString = new AppSettingsReader().GetValue(Constants.LocalConnectionString, typeof(System.String)).ToString();
        //ConfigurationManager.ConnectionStrings[Constants.LocalConnectionString].ConnectionString;
        private Package _taskpackage;
        private ConnectionManager databaseConnectionManager;
        public ControlFlowTask(Package package)
        {
            _taskpackage = package;
            databaseConnectionManager = SetConnectionManager.CreateOLEDBConnectionManager(_taskpackage, "SSIS Connection Manager for Oledb");
        }

        public void RunControlFlowTask()
        {
            // Add a Data Flow Task to the Package :Control Flow Task
            var e = _taskpackage.Executables.Add("STOCK:PipelineTask");
            var mainPipe = e as TaskHost;
            //string[] columns = null;

            if (mainPipe != null)
            {
                mainPipe.Name = "SynkrinoDataFlowTask";
                var dataFlowTask = mainPipe.InnerObject as MainPipe;
                // mainPipe.DelayValidation = true;
                List<string> PrimaryColumn = Utility.removeDuplicates(new string[] { "EMP_ID", "COMPANY_ID" }.ToList());
                var app = new Application();

                if (dataFlowTask != null)
                {
                    AddVaribles();
                    ComponentEventHandler events = new ComponentEventHandler();
                    dataFlowTask.Events = DtsConvert.GetExtendedInterface(events as IDTSComponentEvents);

                    //TODO: need to remove the Flat file connection.
                    //  FlatFileComponent.CreateFlatFile(_taskpackage, dataFlowTask, app, "SourceFileConnection", "SourceFile", ConfigurationManager.AppSettings["SSISPATH"]);
                    string DestinationEXCELPATH = new AppSettingsReader().GetValue("DestinationEXCELPATH", typeof(System.String)).ToString();
                    //Creating Excel sources for destination  Excel
                    IDTSDesigntimeComponent100 destinationExcelWrapper;
                    IDTSComponentMetaData100 destinationExcelMetaDataobj = OLEDBComponent.CreateExcelComponent(_taskpackage, dataFlowTask, app, out destinationExcelWrapper, "Destination Excel", "Book1$", "Destination OLEDB EXCEL", Constants.DTSAdapterExcelSource, DestinationEXCELPATH);
                    destinationExcelMetaDataobj.OutputCollection[0].IsSorted = true;
                    destinationExcelMetaDataobj.OutputCollection[0].OutputColumnCollection[0].SortKeyPosition = 1;
                    destinationExcelMetaDataobj.OutputCollection[0].OutputColumnCollection[1].SortKeyPosition = 2;

                    string sourceEXCELPATH = new AppSettingsReader().GetValue("sourceEXCELPATH", typeof(System.String)).ToString();
                    //Creating Excel sources for source Excel
                    IDTSDesigntimeComponent100 sourceExcelWrapper;
                    IDTSComponentMetaData100 sourceExcelMetaDataobj = OLEDBComponent.CreateExcelComponent(_taskpackage, dataFlowTask, app, out sourceExcelWrapper, "Source Excel", "Book1$", "Source OLEDB EXCEL", Constants.DTSAdapterExcelSource, sourceEXCELPATH);
                    sourceExcelMetaDataobj.OutputCollection[0].IsSorted = true;
                    sourceExcelMetaDataobj.OutputCollection[0].OutputColumnCollection[0].SortKeyPosition = 1;
                    sourceExcelMetaDataobj.OutputCollection[0].OutputColumnCollection[1].SortKeyPosition = 2;



                    //Adding row count
                    IDTSComponentMetaData100 rowcountMetadata = OLEDBComponent.GetRowcount(_taskpackage, dataFlowTask, app, sourceExcelMetaDataobj, "get row counter");
                    //Derived Column for sources file
                    CManagedComponentWrapper sourceDerivedColumnsWrapper;
                    IDTSComponentMetaData100 sourcederivedMetaDataobj = DrivedColumn.CreateDrivedColumn(dataFlowTask, rowcountMetadata, out sourceDerivedColumnsWrapper, "DER_SRC_EMPLOYEE", PrimaryColumn);

                    var Expression = new StringBuilder();

                    Expression.Append("(DT_STR,50,1252)[EMP_ID] +");
                    Expression.Append(@"""-""");
                    Expression.Append("+ (DT_STR,50,1252)[COMPANY_ID]");
                    string expressionValue = Expression.ToString();

                    DrivedColumn.ImplementingDerivedCoulmn(sourcederivedMetaDataobj, "SRC_PRIMARY_KEYS", expressionValue);
                    expressionValue = "@OUTPUT_ID";
                    DrivedColumn.ImplementingDerivedCoulmn(sourcederivedMetaDataobj, "SRC_OUTPUT_ID", expressionValue);
                    expressionValue = "@[LOAD_TS]";
                    DrivedColumn.ImplementingDerivedCoulmn(sourcederivedMetaDataobj, "SRC_LOAD_TS", expressionValue);

                    //Derived Column for destination file
                    CManagedComponentWrapper destinationDerivedColumnsWrapper;
                    IDTSComponentMetaData100 destinationderivedMetaDataobj = DrivedColumn.CreateDrivedColumn(dataFlowTask, destinationExcelMetaDataobj, out destinationDerivedColumnsWrapper, "DER_DST_EMPLOYEE", PrimaryColumn);
                    //DrivedColumn.ImplementDerivedCoulmn(destinationderivedMetaDataobj, "PRIMARY_KEYS", "OUTPUT_ID", "LOAD_TS");
                    expressionValue = Expression.ToString();
                    DrivedColumn.ImplementingDerivedCoulmn(destinationderivedMetaDataobj, "PRIMARY_KEYS", expressionValue);
                    expressionValue = "@OUTPUT_ID";
                    DrivedColumn.ImplementingDerivedCoulmn(destinationderivedMetaDataobj, "OUTPUT_ID", expressionValue);
                    expressionValue = "@[LOAD_TS]";
                    DrivedColumn.ImplementingDerivedCoulmn(destinationderivedMetaDataobj, "LOAD_TS", expressionValue);

                    //Add MergeJoin
                    IDTSComponentMetaData100 MergeMetaDataComponent = MergeJoinComponent.MergeJoin(dataFlowTask, "MERGE_EMPLOYEE", sourcederivedMetaDataobj, destinationderivedMetaDataobj);
                    // Add an ConditionalSplit to the data flow.
                    IDTSComponentMetaData100 conditionalSplitMetaDataComponent = ConditionalSplitComponent.ConditionalSplit(dataFlowTask, MergeMetaDataComponent.OutputCollection[0], "COND_EMPLOYEE", new string[] { "DST_EMP_ID", "DST_COMPANY_ID", "EMP_ID", "COMPANY_ID" });
                    //Adding Expression for the component.
                    string expressionvalue = string.Empty;
                    expressionvalue = "!ISNULL(DST_EMP_ID) && !ISNULL(DST_COMPANY_ID) && !ISNULL(EMP_ID) && !ISNULL(COMPANY_ID)";
                    ConditionalSplitComponent.ConditionalSplitFilerCondtion(conditionalSplitMetaDataComponent, "ColumnValidation", expressionvalue, "0");
                    expressionvalue = "ISNULL(DST_EMP_ID) && ISNULL(DST_COMPANY_ID) && !ISNULL(EMP_ID) && !ISNULL(COMPANY_ID)";
                    ConditionalSplitComponent.ConditionalSplitFilerCondtion(conditionalSplitMetaDataComponent, "Missing_Rows_Destination", expressionvalue, "1");
                    expressionvalue = "!ISNULL(DST_EMP_ID) && !ISNULL(DST_COMPANY_ID) && ISNULL(EMP_ID) && ISNULL(COMPANY_ID)";
                    ConditionalSplitComponent.ConditionalSplitFilerCondtion(conditionalSplitMetaDataComponent, "Missing_Rows_Source", expressionvalue, "2");

                    string destinationSP = @"EXEC dbo.USP_OUTPUT @OUTPUT_ID = ?,@PRIMARY_KEYS = ?,@MISSING_IN_DST = 0,@MISSING_IN_SRC = 1,@FILE_NAME ='DST_EMPLOYEE',@LOAD_TS = ?";
                    CManagedComponentWrapper oleDbDestinationWrapper1;
                    IDTSComponentMetaData100 destinationOleDb1 = OLEDBComponent.CreateOLEDBCommand(_taskpackage, dataFlowTask, app, out oleDbDestinationWrapper1, "OLEDB_MISSING_IN_Destination", destinationSP, "OLEDB Traget Destination", Operator.SQLSP, Constants.DTSTransformOLEDBCommand, conditionalSplitMetaDataComponent.OutputCollection[4], "DST_", databaseConnectionManager);


                    //CManagedComponentWrapper oleDbDestinationWrapper;
                    //IDTSComponentMetaData100 destinationOleDb = OLEDBComponent.CreateOLEDBComponent(_taskpackage, dataFlowTask, app, out oleDbDestinationWrapper, "OLEDB_MISSING_IN_SRC", "Select * from Orders", "OLEDB Source Destination", Operator.SQLCOMMAND, Constants.DTSOleDbDestination, conditionalSplitMetaDataComponent, 4);


                    string sourceSP = @"EXEC dbo.USP_OUTPUT @OUTPUT_ID = ?,@PRIMARY_KEYS = ?,@MISSING_IN_DST = 0,@MISSING_IN_SRC = 1,@FILE_NAME = 'SRC_EMPLOYEE',@LOAD_TS = ?";
                    CManagedComponentWrapper oleDbDestinationWrapper2;
                    IDTSComponentMetaData100 destinationOleDb2 = OLEDBComponent.CreateOLEDBCommand(_taskpackage, dataFlowTask, app, out oleDbDestinationWrapper2, "OLEDB_MISSING_IN_Source", sourceSP, "OLEDB Traget Source", Operator.SQLSP, Constants.DTSTransformOLEDBCommand, conditionalSplitMetaDataComponent.OutputCollection[3], "SRC_", databaseConnectionManager);

                    //Adding Multicasting 
                    CManagedComponentWrapper multicastWrapper;
                    IDTSComponentMetaData100 multicastMetaData = MulticastComponent.CreateMulticast(dataFlowTask, "Multicast", out multicastWrapper, conditionalSplitMetaDataComponent.OutputCollection[2]);

                    // Add an ConditionalSplit to the data flow.
                    IDTSComponentMetaData100 SplitCOND_NAME = ConditionalSplitComponent.ConditionalSplit(dataFlowTask, multicastMetaData.OutputCollection[0], "COND_NAME", new string[] { "NAME", "DST_DESCRIPTION" });

                    expressionvalue = "NAME != DST_DESCRIPTION";
                    ConditionalSplitComponent.ConditionalSplitFilerCondtion(SplitCOND_NAME, "Name Validation", expressionvalue, "0");
                    string NameSP = @"EXEC dbo.USP_OUTPUT @OUTPUT_ID = ?,@PRIMARY_KEYS = ?,@SRC_COLUMN_NAME = 'NAME',@SRC_COLUMN_VALUE = ?,@DST_COLUMN_NAME = 'DST_DESCRIPTION',@DST_COLUMN_VALUE = ?,@MISSING_IN_SRC = 0,@MISSING_IN_DST = 0,@FILE_NAME = 'DST_EMPLOYEE',@LOAD_TS = ?";
                    CManagedComponentWrapper nameWrapper;
                    IDTSComponentMetaData100 nameMetaData100 = OLEDBComponent.CreateOLEDBCommand(_taskpackage, dataFlowTask, app, out nameWrapper, "OLEDB_INSERT_NAME", NameSP, "Multi Traget Destination", Operator.SQLSP, Constants.DTSTransformOLEDBCommand, SplitCOND_NAME.OutputCollection[0], "DST_|NAME|DST_DESCRIPTION", databaseConnectionManager);



                    // Add an ConditionalSplit to the data flow.
                    IDTSComponentMetaData100 SplitCOND_AGE = ConditionalSplitComponent.ConditionalSplit(dataFlowTask, multicastMetaData.OutputCollection[1], "COND_AGE", new string[] { "AGE", "DST_NO_OF_YEARS" });
                    //AGE != NO_OF_YEARS
                    expressionvalue = "AGE != DST_NO_OF_YEARS";
                    ConditionalSplitComponent.ConditionalSplitFilerCondtion(SplitCOND_AGE, "AGE Validation", expressionvalue, "0");

                    string AgeSP = @"EXEC dbo.USP_OUTPUT @OUTPUT_ID = ?,@PRIMARY_KEYS = ?,@SRC_COLUMN_NAME = 'Age',@SRC_COLUMN_VALUE = ?,@DST_COLUMN_NAME = 'NO_OF_YEARS',@DST_COLUMN_VALUE = ?,@MISSING_IN_SRC = 0,@MISSING_IN_DST = 0,@FILE_NAME = 'DST_EMPLOYEE',@LOAD_TS = ?";
                    CManagedComponentWrapper WrapperAge;
                    IDTSComponentMetaData100 ageMetaData100 = OLEDBComponent.CreateOLEDBCommand(_taskpackage, dataFlowTask, app, out WrapperAge, "OLDDB_INSERT_AGE", AgeSP, "Multi Traget Destination2", Operator.SQLSP, Constants.DTSTransformOLEDBCommand, SplitCOND_AGE.OutputCollection[0], "DST_|AGE|DST_NO_OF_YEARS", databaseConnectionManager);

                    // Add an ConditionalSplit to the data flow.
                    IDTSComponentMetaData100 SplitCOND_DEPARTMENT = ConditionalSplitComponent.ConditionalSplit(dataFlowTask, multicastMetaData.OutputCollection[2], "COND_DEPARTMENT", new string[] { "DEPARTMENT", "DST_BRANCH" });
                    //
                    expressionvalue = "DEPARTMENT != DST_BRANCH";
                    ConditionalSplitComponent.ConditionalSplitFilerCondtion(SplitCOND_DEPARTMENT, "DEPARTMENT Validation", expressionvalue,"0");

                    string departmentSP = @"EXEC dbo.USP_OUTPUT @OUTPUT_ID = ?,@PRIMARY_KEYS = ?,@SRC_COLUMN_NAME = 'Department',@SRC_COLUMN_VALUE = ?,@DST_COLUMN_NAME = 'BRANCH',@DST_COLUMN_VALUE = ?,@MISSING_IN_SRC = 0,@MISSING_IN_DST = 0,@FILE_NAME = 'DST_EMPLOYEE',@LOAD_TS = ?";
                    CManagedComponentWrapper departmentWrapper;
                    IDTSComponentMetaData100 departmentMetaData100 = OLEDBComponent.CreateOLEDBCommand(_taskpackage, dataFlowTask, app, out departmentWrapper, "OLDDB_INSERT_DEPARTMENT", departmentSP, "Multi Traget Destination3", Operator.SQLSP, Constants.DTSTransformOLEDBCommand, SplitCOND_DEPARTMENT.OutputCollection[0], "DST_|DEPARTMENT|DST_BRANCH", databaseConnectionManager);

                }

                Console.WriteLine("Executing Package...");
                // package.Execute();
                string _filePath= new AppSettingsReader().GetValue("SSISPATH", typeof(System.String)).ToString();

                var dtsx = new StringBuilder();
                dtsx.Append(Path.GetDirectoryName(_filePath)).Append("\\").Append("Synkrino_").Append(DateTime.Now.ToString("hhmmss")).Append(".dtsx");

                // app.SaveToXml(string.Format(@"c:\DB\LookupWithDrived{0}.dtsx", DateTime.Now.ToString("hhmmss")), pkg, null); ConfigurationManager.AppSettings["EXCELPATH"]
                Console.WriteLine("Saving Package...");
                app.SaveToXml(dtsx.ToString(), _taskpackage, null);
                Console.WriteLine("Package created Pres Enter...");
            }
        }

       

        private void AddVaribles()
        {
            //string namespaceName = Assembly.GetExecutingAssembly().GetName().Name;
            string namespaceName = "";
            int tryval = 0;
            CultureInfo ci = CultureInfo.InvariantCulture;
           // _taskpackage.Variables.Add("LOAD_TS", false, namespaceName, DateTime.ParseExact("11/26/2016", "MM/dd/yyyy", ci));
            AddVariblesandExpression("LOAD_TS", false,DateTime.ParseExact("11/26/2016", "MM/dd/yyyy", ci), namespaceName, "GETDATE()");
            AddVariblesandExpression("OUTPUT_ID", false, 0, namespaceName, string.Empty);
            //_taskpackage.Variables.Add("OUTPUT_ID", false, namespaceName, 0);
            //_taskpackage.Variables.Add("CONN_STRING_OUTPUT_DB", false, namespaceName, ConfigurationManager.AppSettings[Constants.CONN_STRING_OUTPUT_DB]);
            //_taskpackage.Variables.Add("FILE_PATH_DST_EMP_ADDRESS", false, namespaceName, ConfigurationManager.AppSettings[Constants.FILE_PATH_DST_EMP_ADDRESS]);
            //_taskpackage.Variables.Add("FILE_PATH_DST_EMPLOYEE", false, namespaceName, ConfigurationManager.AppSettings[Constants.FILE_PATH_DST_EMPLOYEE]);
            //_taskpackage.Variables.Add("FILE_PATH_SRC_EMP_ADDRESS", false, namespaceName, ConfigurationManager.AppSettings[Constants.FILE_PATH_SRC_EMP_ADDRESS]);
            //_taskpackage.Variables.Add("FILE_PATH_SRC_EMPLOYEE", false, namespaceName, ConfigurationManager.AppSettings[Constants.FILE_PATH_SRC_EMPLOYEE]);
            //_taskpackage.Variables.Add("VAR_EMP_ADDRESS_COUNT", false, namespaceName, int.TryParse(ConfigurationManager.AppSettings[Constants.VAR_EMP_ADDRESS_COUNT], out tryval));
            //_taskpackage.Variables.Add("VAR_EMPLOYEE_COUNT", false, namespaceName, int.TryParse(ConfigurationManager.AppSettings[Constants.VAR_EMPLOYEE_COUNT], out tryval));
            //_taskpackage.Variables.Add("VAR_EMP_DETAILS_COUNT", false, namespaceName, int.TryParse(ConfigurationManager.AppSettings[Constants.VAR_EMP_DETAILS_COUNT], out tryval));
            // AddVariblesandExpression("VAR_TOTAL_COUNT",false, int.TryParse(ConfigurationManager.AppSettings[Constants.VAR_TOTAL_COUNT], out tryval), namespaceName, "@VAR_EMPLOYEE_COUNT + @[User::VAR_EMP_DETAILS_COUNT] +  @VAR_EMP_ADDRESS_COUNT");

        }

        private void AddVariblesandExpression(string varibleName,bool ronly,object val,string namespaceName,string expression)
        {
            Variable v = null;
            v = _taskpackage.Variables.Add(varibleName, false, namespaceName, val);
            v.EvaluateAsExpression = true;
            v.Expression = expression;
           
        }
    }
}
