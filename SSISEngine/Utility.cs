using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace SSISEngine
{
    public class Utility
    {
        private Dictionary<string, int> outputColumnLineageIDs = new Dictionary<string, int>();
        /// <summary>
        /// 
        /// </summary>
        /// <param name="p"></param>
        /// <returns></returns>
        private object GetSourceColumnLineageID(string p)
        {
            if (outputColumnLineageIDs.ContainsKey(p))
            {
                return outputColumnLineageIDs[p];
            }
            else
            {
                return outputColumnLineageIDs["\"" + p + "\""];
            }
        }

        public static string GetappSettings(string key)
        {
            string path = Path.GetDirectoryName(Assembly.GetCallingAssembly().CodeBase) + @"\SSISEngine.dll.config";

            XDocument doc = XDocument.Load(path);

            var query = doc.Descendants("appSettings").Nodes().Cast<XElement>().Where(x => x.Attribute("key").Value.ToString() == key).FirstOrDefault();

            if (query != null)
            {
                return query.Attribute("value").Value.ToString();
            }else
            { return null; }

        }

      public  static List<string> removeDuplicates(List<string> inputList)
        {
            Dictionary<string, int> uniqueStore = new Dictionary<string, int>();
            List<string> finalList = new List<string>();
            foreach (string currValue in inputList)
            {
                if (!uniqueStore.ContainsKey(currValue))
                {   
                    uniqueStore.Add(currValue, 0);
                    finalList.Add(currValue);
                }
            }
            return finalList;
        }
    }
}
