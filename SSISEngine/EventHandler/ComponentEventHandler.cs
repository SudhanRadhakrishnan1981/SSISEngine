﻿using Microsoft.SqlServer.Dts.Runtime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SSISEngine.EventHandler
{
    class ComponentEventHandler : IDTSComponentEvents
    {
        private void HandleEvent(string type, string subComponent, string description)
        {
            Console.WriteLine("[{0}] {1}: {2}", type, subComponent, description);
        }

        #region IDTSComponentEvents Members

        public void FireBreakpointHit(BreakpointTarget breakpointTarget)
        {
        }

        public void FireCustomEvent(string eventName, string eventText, ref object[] arguments, string subComponent, ref bool fireAgain)
        {
        }

        public bool FireError(int errorCode, string subComponent, string description, string helpFile, int helpContext)
        {
            HandleEvent("Error", subComponent, description);
            return true;
        }

        public void FireInformation(int informationCode, string subComponent, string description, string helpFile, int helpContext, ref bool fireAgain)
        {
            HandleEvent("Information", subComponent, description);
        }

        public void FireProgress(string progressDescription, int percentComplete, int progressCountLow, int progressCountHigh, string subComponent, ref bool fireAgain)
        {
        }

        public bool FireQueryCancel()
        {
            return true;
        }

        public void FireWarning(int warningCode, string subComponent, string description, string helpFile, int helpContext)
        {
            HandleEvent("Warning", subComponent, description);
        }

        #endregion
    }
}

