using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime;
using SSISEngine.EventHandler;
using Synkrino.Billing;
using Synkrino.ReportProducerEngine;
using Synkrino.StrategyBuilder;
using Synkrino.WorkPackageDefinition;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Text;
using System.Linq;
using System.Globalization;
using Synkrino.Common;

using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.Management.IntegrationServices;
using pac = Microsoft.SqlServer.Dts.Runtime;
using System.Data.SqlClient;
using System.Data;

namespace SSISEngine
{
    public class CsvToCsvCompare : ICompareType
    {
        private Package _taskpackage;
        private readonly string _connectionString = new AppSettingsReader().GetValue(Constants.LocalConnectionString, typeof(System.String)).ToString();
        private readonly string _ssisconnectionString = new AppSettingsReader().GetValue(Constants.SSISConnectionString, typeof(System.String)).ToString();
        private ConnectionManager databaseConnectionManager;
        private string RequestId = string.Empty;
        
        public void Compare(Fixture fixture, RuleEngine ruleEngine, ReportEngine reportEngine, BillingEngine billingEngine,string requestId,int ExecutionTaskId)
        {
            var app = new Application();
            // Create a new SSIS Package
            _taskpackage = new Package();
            // Add a Data Flow Task to the Package :Control Flow Task
            var e = _taskpackage.Executables.Add("STOCK:PipelineTask");
            var mainPipe = e as TaskHost;
            RequestId = requestId;
            string primarycolumn = string.Empty;
            
            try
            {


                //var dataSourceFactory = new CsvDataSourceFactory();
                var sourceHost = fixture.SourceHost;
                var targetHost = fixture.TargetHost;
                List<Skynkrino.DataTransformationDefinition.Operation> operations = (List<Skynkrino.DataTransformationDefinition.Operation>)fixture.Rules.FirstOrDefault().Operations;
                var isKeyCompareRuleDefined = operations.Any(p => p.Type == OperationType.KeyCompare);

                List<string> PrimaryColumns = Utility.removeDuplicates(fixture.SourceKeyColumns);
               // PrimaryColumns.Add("PRODUCTNAME");//TODO:This Code is for Testing.

                List<string> MapingColumns = Utility.removeDuplicates(fixture.SourceKeyColumns);
               
             
                //string[] columns = null;

                if (mainPipe != null)
                {
                    mainPipe.Name = "SynkrinoDataFlowTask";
                    var dataFlowTask = mainPipe.InnerObject as MainPipe;
                    // mainPipe.DelayValidation = true;



                    if (dataFlowTask != null)
                    {
                        AddVaribles();
                        ComponentEventHandler events = new ComponentEventHandler();
                        dataFlowTask.Events = DtsConvert.GetExtendedInterface(events as IDTSComponentEvents);
                        databaseConnectionManager = SetConnectionManager.CreateOLEDBConnectionManager(_taskpackage, "SSIS Connection Manager for Oledb");

                        IDTSComponentMetaData100 sourceMetaDataobj = FlatFileComponent.CreateFlatFile(_taskpackage, dataFlowTask, app, "DestinationFileConnection", "Destination", targetHost.ConnectionOrPath);

                        sourceMetaDataobj.OutputCollection[0].IsSorted = true;
                        sourceMetaDataobj.OutputCollection[0].OutputColumnCollection[0].SortKeyPosition = 1;


                        IDTSComponentMetaData100 TragetMetaDataobj = FlatFileComponent.CreateFlatFile(_taskpackage, dataFlowTask, app, "sourceFileConnection", "Source", sourceHost.ConnectionOrPath);
                        TragetMetaDataobj.OutputCollection[0].IsSorted = true;
                        TragetMetaDataobj.OutputCollection[0].OutputColumnCollection[0].SortKeyPosition = 1;


                        var Expression = new StringBuilder();

                        int columnCount = PrimaryColumns.Count;
                        for (int i = 0; i < columnCount; i++)
                        {
                            primarycolumn = PrimaryColumns[i].ToString();
                            Expression.Append("(DT_STR,50,1252)");
                            Expression.Append(PrimaryColumns[i].ToUpper());
                            if (PrimaryColumns.Count > (1 + i))
                            {
                                Expression.Append(" + ");
                                Expression.Append(@"""-""");
                                Expression.Append("+ ");
                            }
                            else
                            {
                                Expression.Append(" + ");
                                Expression.Append(@"""-""");
                                Expression.Append("+ ");
                                Expression.Append(@"""");
                                Expression.Append(primarycolumn);
                                Expression.Append(@"""");
                            }
                        }

                        string expressionValue = Expression.ToString();

                        //Derived Column for sources file
                        CManagedComponentWrapper sourceDerivedColumnsWrapper;
                        IDTSComponentMetaData100 sourcederivedMetaDataobj = DrivedColumn.CreateDrivedColumn(dataFlowTask, TragetMetaDataobj, out sourceDerivedColumnsWrapper, "DER_SRC_ORDER", PrimaryColumns);


                        DrivedColumn.ImplementingDerivedCoulmn(sourcederivedMetaDataobj, "PRIMARY_KEYS", expressionValue);
                        expressionValue = "@REPORT_ID";
                        DrivedColumn.ImplementingDerivedCoulmn(sourcederivedMetaDataobj, "REPORT_ID", expressionValue);
                        expressionValue = "@[LOAD_TS]";
                        DrivedColumn.ImplementingDerivedCoulmn(sourcederivedMetaDataobj, "LOAD_TS", expressionValue);

                        //Derived Column for destination file
                        CManagedComponentWrapper destinationDerivedColumnsWrapper;
                        IDTSComponentMetaData100 destinationderivedMetaDataobj = DrivedColumn.CreateDrivedColumn(dataFlowTask, sourceMetaDataobj, out destinationDerivedColumnsWrapper, "DER_DST_ORDER", PrimaryColumns);

                        expressionValue = Expression.ToString();
                        DrivedColumn.ImplementingDerivedCoulmn(destinationderivedMetaDataobj, "PRIMARY_KEYS", expressionValue);
                        expressionValue = "@REPORT_ID";
                        DrivedColumn.ImplementingDerivedCoulmn(destinationderivedMetaDataobj, "REPORT_ID", expressionValue);
                        expressionValue = "@[LOAD_TS]";
                        DrivedColumn.ImplementingDerivedCoulmn(destinationderivedMetaDataobj, "LOAD_TS", expressionValue);


                        //Add MergeJoin
                        IDTSComponentMetaData100 MergeMetaDataComponent = MergeJoinComponent.MergeJoin(dataFlowTask, "MERGE_EMPLOYEE", destinationderivedMetaDataobj, sourcederivedMetaDataobj);
                        // Add an ConditionalSplit to the data flow.
                        IDTSComponentMetaData100 conditionalSplitMetaDataComponent = ConditionalSplitComponent.ConditionalSplit(dataFlowTask, MergeMetaDataComponent.OutputCollection[0], "COND_ORDER", new string[] { "DST_ORDERNO", "ORDERNO", });
                        //Adding Expression for the component.
                        string expressionvalue = string.Empty;
                        expressionvalue = "!ISNULL(DST_"+ primarycolumn + ") && !ISNULL("+ primarycolumn + ")";
                        ConditionalSplitComponent.ConditionalSplitFilerCondtion(conditionalSplitMetaDataComponent, "ColumnValidation", expressionvalue, "0");
                        expressionvalue = "ISNULL(DST_"+ primarycolumn + ")&& !ISNULL("+ primarycolumn + ")";
                        ConditionalSplitComponent.ConditionalSplitFilerCondtion(conditionalSplitMetaDataComponent, "Missing_Rows_Source", expressionvalue, "1");
                        expressionvalue = "!ISNULL(DST_"+ primarycolumn + ")  && ISNULL("+ primarycolumn + ")";
                        ConditionalSplitComponent.ConditionalSplitFilerCondtion(conditionalSplitMetaDataComponent, "Missing_Rows_Destination", expressionvalue, "2");

                        string destinationSP = @"EXEC dbo.USP_SSISOUTPUT @REPORT_ID = ?,@PRIMARY_KEYS = ?,@MISSING_IN_DST = 1,@MISSING_IN_SRC = 0,@COLUMN_VALUE_DIFF =0,@FILE_NAME ='DST_EMPLOYEE',@LOAD_TS = ?";
                        CManagedComponentWrapper oleDbDestinationWrapper1;
                        IDTSComponentMetaData100 destinationOleDb1 = OLEDBComponent.CreateOLEDBCommand(_taskpackage, dataFlowTask, app, out oleDbDestinationWrapper1, "OLEDB_MISSING_IN_Destination", destinationSP, "OLEDB Traget Destination", Operator.SQLSP, Constants.DTSTransformOLEDBCommand, conditionalSplitMetaDataComponent.OutputCollection[4], "DST_", databaseConnectionManager);


                        string sourceSP = @"EXEC dbo.USP_SSISOUTPUT @REPORT_ID = ?,@PRIMARY_KEYS = ?,@MISSING_IN_DST = 0,@MISSING_IN_SRC = 1,@COLUMN_VALUE_DIFF =0,@FILE_NAME = 'SRC_EMPLOYEE',@LOAD_TS = ?";
                        CManagedComponentWrapper oleDbDestinationWrapper2;
                        IDTSComponentMetaData100 destinationOleDb2 = OLEDBComponent.CreateOLEDBCommand(_taskpackage, dataFlowTask, app, out oleDbDestinationWrapper2, "OLEDB_MISSING_IN_Source", sourceSP, "OLEDB Traget Source", Operator.SQLSP, Constants.DTSTransformOLEDBCommand, conditionalSplitMetaDataComponent.OutputCollection[3], "", databaseConnectionManager);

                        //Adding Multicasting 
                        CManagedComponentWrapper multicastWrapper;
                        IDTSComponentMetaData100 multicastMetaData = MulticastComponent.CreateMulticast(dataFlowTask, "Multicast", out multicastWrapper, conditionalSplitMetaDataComponent.OutputCollection[2]);
                        int multicast = 0;
                        //
                        foreach (Skynkrino.DataTransformationDefinition.Operation operation in operations)
                        {
                            // Add an ConditionalSplit to the data flow.
                            IDTSComponentMetaData100 SplitCOND_NAME = ConditionalSplitComponent.ConditionalSplit(dataFlowTask, multicastMetaData.OutputCollection[multicast], "COND_" + operation.SourceColumn, new string[] { operation.SourceColumn, Constants.Destination + operation.TargetColumn });
                            string mapingstring = Constants.Destination + Constants.columnDelimiter + operation.SourceColumn + Constants.columnDelimiter + Constants.Destination + operation.TargetColumn;
                            expressionvalue = operation.SourceColumn + Constants.Condiexpression + Constants.Destination + operation.TargetColumn;
                            ConditionalSplitComponent.ConditionalSplitFilerCondtion(SplitCOND_NAME, operation.SourceColumn+ " Validation", expressionvalue, "0");
                            expressionvalue = string.Empty;
                            expressionvalue = operation.SourceColumn + Constants.CondiEqual  + Constants.Destination + operation.TargetColumn;
                            ConditionalSplitComponent.ConditionalSplitFilerCondtion(SplitCOND_NAME, operation.SourceColumn + " Val", expressionvalue, "1");

                            //Creating SP for inserting data.
                            StringBuilder storedProcedure = new StringBuilder();
                            storedProcedure.Append("EXEC dbo.USP_SSISOUTPUT @REPORT_ID = ?,@PRIMARY_KEYS = ?,@SRC_COLUMN_NAME = '");
                            storedProcedure.Append(operation.SourceColumn);
                            storedProcedure.Append("',@SRC_COLUMN_VALUE = ?,@DST_COLUMN_NAME = '");
                            storedProcedure.Append(operation.TargetColumn);
                            storedProcedure.Append("',@DST_COLUMN_VALUE = ?,@MISSING_IN_SRC = 0,@MISSING_IN_DST = 0,@COLUMN_VALUE_DIFF =1,@FILE_NAME = '");
                            storedProcedure.Append("DST_EMPLOYEE");
                            storedProcedure.Append("',@LOAD_TS = ?");
                            string NameSP = storedProcedure.ToString();
                            CManagedComponentWrapper nameWrapper;
                            IDTSComponentMetaData100 nameMetaData100 = OLEDBComponent.CreateOLEDBCommand(_taskpackage, dataFlowTask, app, out nameWrapper, "MISMATCHED_" + operation.SourceColumn, NameSP, "Multi Traget Destination", Operator.SQLSP, Constants.DTSTransformOLEDBCommand, SplitCOND_NAME.OutputCollection[2], mapingstring, databaseConnectionManager);
                            NameSP = string.Empty;
                            //Creating SP for inserting data.
                            StringBuilder storedProcedureMatched = new StringBuilder();
                            storedProcedureMatched.Append("EXEC dbo.USP_SSISOUTPUT @REPORT_ID = ?,@PRIMARY_KEYS = ?,@SRC_COLUMN_NAME = '");
                            storedProcedureMatched.Append(operation.SourceColumn);
                            storedProcedureMatched.Append("',@SRC_COLUMN_VALUE = ?,@DST_COLUMN_NAME = '");
                            storedProcedureMatched.Append(operation.TargetColumn);
                            storedProcedureMatched.Append("',@DST_COLUMN_VALUE = ?,@MISSING_IN_SRC = 0,@MISSING_IN_DST = 0,@COLUMN_VALUE_DIFF =0,@FILE_NAME = '");
                            storedProcedureMatched.Append("DST_EMPLOYEE");
                            storedProcedureMatched.Append("',@LOAD_TS = ?");
                            NameSP = storedProcedureMatched.ToString();

                            CManagedComponentWrapper nameWrappermatched;
                            IDTSComponentMetaData100 nameMetaData100matched = OLEDBComponent.CreateOLEDBCommand(_taskpackage, dataFlowTask, app, out nameWrappermatched, "MATCHED_" + operation.SourceColumn, NameSP, "Matched Traget Destination", Operator.SQLSP, Constants.DTSTransformOLEDBCommand, SplitCOND_NAME.OutputCollection[3], mapingstring, databaseConnectionManager);



                            multicast++;
                        }
                        
                    }

                }
                Console.WriteLine("Executing Package...");
               // _taskpackage.Execute();

                string ssisLocation = new AppSettingsReader().GetValue("SSISPATH", typeof(System.String)).ToString();
                var dtsx = new StringBuilder();
                var filename = new StringBuilder();
                filename = filename.Append("Synkrino_").Append(DateTime.Now.ToString("hhmmss")).Append(".dtsx");
                dtsx.Append(Path.GetDirectoryName(ssisLocation)).Append("\\").Append(filename.ToString());
                app.SaveToXml(dtsx.ToString(), _taskpackage, null);
                deploySSIS(_taskpackage, filename.ToString());
                //need to create a SP and call it from C#
                ExecuteSSISPcak(filename.ToString(), RequestId);

            }
            catch (Exception ex)
            {
                Console.Write(ex.Message.ToString());
                mainPipe.Dispose();
                _taskpackage.Dispose();
                app = null;
            }
            finally
            {
                mainPipe.Dispose();
                _taskpackage.Dispose();
                app = null;
            }

        }

        private string createSP()
        {
            StringBuilder sbSP = new StringBuilder();
            sbSP.AppendLine("CREATE PROCEDURE spExecute_SSIS_pack ");
            sbSP.AppendLine(" @FolderName nvarchar(128) ");
            sbSP.AppendLine(",@ProjectName nvarchar(128) ");
            sbSP.AppendLine(",@PackageName nvarchar(260) ");
            sbSP.AppendLine("As ");
            sbSP.AppendLine(" begin ");
            sbSP.AppendLine(" declare @execution_id bigint ");
            sbSP.AppendLine("EXEC[SSISDB].[catalog].[create_execution] ");
            sbSP.AppendLine(" @package_name=@PackageName ");
            sbSP.AppendLine(",@execution_id=@execution_id OUTPUT");
            sbSP.AppendLine(", @folder_name = @FolderName ");
            sbSP.AppendLine(",@project_name=@ProjectName ");
            sbSP.AppendLine(",@use32bitruntime=False");
            sbSP.AppendLine(",@reference_id=Null ");
            sbSP.AppendLine("Select @execution_id ");
            sbSP.AppendLine("DECLARE @var0 smallint = 1 ");
            sbSP.AppendLine(" EXEC [SSISDB].[catalog].[set_execution_parameter_value] ");
            sbSP.AppendLine(" @execution_id ");
            sbSP.AppendLine(",@object_type=50 ");
            sbSP.AppendLine(",@parameter_name=N'LOGGING_LEVEL' ");
            sbSP.AppendLine(",@parameter_value=@var0 ");
            sbSP.AppendLine(" EXEC [SSISDB].[catalog].[start_execution] ");
            sbSP.AppendLine("@execution_id ");
            sbSP.AppendLine(" end ");
            return sbSP.ToString();
        }
        private string removeDuplicate(string requestid)
        {
            StringBuilder sbSP = new StringBuilder();
            sbSP.AppendLine("DELETE FROM TBL_SSISOUTPUT ");
            sbSP.AppendLine("  WHERE REPORT_ID = ' ");
            sbSP.AppendLine(requestid);
            sbSP.AppendLine("'");           
            return sbSP.ToString();
        }

        private void ExecuteSSISPcak(string PackageName,string RequestId)
        {
            string SSISServerName = @"E519LTRV\MSSQLSERVER1";
            string sqlConnectionString = "Data Source=" + SSISServerName +
               ";Initial Catalog=SSISDB;Integrated Security=SSPI;";
            string sqlConnectionStringSandbox = "Data Source=" + SSISServerName +
              ";Initial Catalog=Sandbox;Integrated Security=SSPI;";

            string SSIS_pack = createSP();
            //string removeduplicate = removeDuplicate(RequestId);


            //using (SqlConnection con = new SqlConnection(sqlConnectionStringSandbox))
            //{
            //    using (SqlCommand cmd = new SqlCommand(removeduplicate.ToString(), con))
            //    {
            //        con.Open();
            //        cmd.CommandType = CommandType.Text;
            //        cmd.ExecuteReader();
            //        con.Close();
            //    }
            //}

            using (SqlConnection con = new SqlConnection(sqlConnectionString))
            {
               
                using (SqlCommand cmd = new SqlCommand(SSIS_pack.ToString(), con))
                {
                    con.Open();
                    cmd.CommandType = CommandType.Text;
                    cmd.ExecuteNonQuery();
                    con.Close();
                }
                using (SqlCommand cmd = new SqlCommand("spExecute_SSIS_pack", con))
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.AddWithValue("@FolderName", Constants.FolderName);
                    cmd.Parameters.AddWithValue("@ProjectName", Constants.ProjectName);
                    cmd.Parameters.AddWithValue("@PackageName", PackageName);
                    con.Open();
                    cmd.ExecuteNonQuery();
                    Console.WriteLine("SP Executed !!!:");
                }

            }
            
        }

        static void deploySSIS(pac.Package packg,string filename)
        {
            Server SMO = new Server("E519LTRV\\MSSQLSERVER1");
            IntegrationServices isdemo = new IntegrationServices(SMO);
            Catalog ssCt = new Catalog(isdemo);
            CatalogFolder ssCF = new CatalogFolder(ssCt, "Sample", "Dec");
            SSISobjectCreareion(1, isdemo, packg, filename);
        }

        private static void SSISobjectCreareion(int condition, IntegrationServices ssis,pac.Package packg,string filename)
        {
            if (condition == 1)
            {
                string SSISCatalogName = "SSISDB";
                string SSISCatalogPassword = "Pwd$1234";
                IntegrationServices integrationServices = ssis;
                //Catalog catalog;
                if (integrationServices.Catalogs.Count > 0)
                {
                    Catalog catalog;
                    catalog = ssis.Catalogs["SSISDB"];
                    Console.WriteLine("About to delete existing SSIS Catalog, all the SSIS Projects will be deleted !!!:");
                    //Deleting SSIS Catalog !!!
                    catalog.Drop();
                }

                Catalog Vct = new Catalog(ssis, SSISCatalogName, SSISCatalogPassword);
                Vct.Create();
                Console.WriteLine("Catalog created !!!:");
                CatalogFolder Vctf = new CatalogFolder(Vct, Constants.FolderName, "This is SSIS Synkrino Demo Folder");
                Vctf.Create();
                Console.WriteLine("CatalogFolder created !!!:");

                string projectFilename = @"C:\Temp\SSISDemoProject.ispac";
                using (pac.Project proj = pac.Project.CreateProject(projectFilename))
                {
                    proj.Name = Constants.ProjectName;
                    proj.Description = "This is a Synkrino SSIS Demo";
                    proj.PackageItems.Add(packg, filename);
                    proj.PackageItems[0].Package.Description = "SSIS Synkrino Demo package";
                    proj.Save();
                    Console.WriteLine("packg is added to the  Project !!!:");
                }
                byte[] stream = System.IO.File.ReadAllBytes(projectFilename);
                Vctf.DeployProject(Constants.ProjectName, stream);
                Console.WriteLine("packg Deploy !!!:");
                Vctf.Alter();
            }           
        }
        private void AddVaribles()
        {
            //string namespaceName = Assembly.GetExecutingAssembly().GetName().Name;
            string namespaceName = "";
            int tryval = 0;
            CultureInfo ci = CultureInfo.InvariantCulture;
            // _taskpackage.Variables.Add("LOAD_TS", false, namespaceName, DateTime.ParseExact("11/26/2016", "MM/dd/yyyy", ci));
            AddVariblesandExpression("LOAD_TS", false, DateTime.ParseExact("11/26/2016", "MM/dd/yyyy", ci), namespaceName, "GETDATE()");
            AddVariblesandExpression("OUTPUT_ID", false, RequestId, namespaceName, string.Empty);
            AddVariblesandExpression("REPORT_ID", false, RequestId, namespaceName, string.Empty);
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

        private void AddVariblesandExpression(string varibleName, bool ronly, object val, string namespaceName, string expression)
        {
            Variable v = null;
            v = _taskpackage.Variables.Add(varibleName, false, namespaceName, val);
            v.EvaluateAsExpression = true;
            v.Expression = expression;

        }

    }
}
