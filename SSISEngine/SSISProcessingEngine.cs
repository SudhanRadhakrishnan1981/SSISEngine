using Microsoft.SqlServer.Dts.Runtime;
using Synkrino.Billing;
using Synkrino.ReportProducerEngine;
using Synkrino.StrategyBuilder;
using Synkrino.WorkPackageDefinition;
using System;
using System.Configuration;

namespace SSISEngine
{
    public class SSISProcessingEngine
    {
        public void Run(Fixture fixture, RuleEngine ruleEngine, ReportEngine reportEngine, BillingEngine billingEngine,string RequestId,int ExecutionTaskId)
        {
            var sourceHostType = fixture.SourceHost.HostType;
            var targetHostType = fixture.TargetHost.HostType;
            var compareFactory = new CompareFactory();
            var comparer = compareFactory.GetComparer(sourceHostType, targetHostType);
            comparer.Compare(fixture, ruleEngine, reportEngine, billingEngine, RequestId, ExecutionTaskId);         
                     
        }
    }
}
