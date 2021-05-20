using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Microsoft.AnalysisServices;
using Microsoft.AnalysisServices.Tabular;

namespace RLS_XMLA.net
{
    class Program
    {
        static void Main(string[] args)
        {
            DynamicQueryParser("powerbi://api.powerbi.com/v1.0/myorg/powerbiembeddedGEN2Dev"
              , "DatabaseName", "app:UserId",
              "Password", "EVALUATE 'SalesLT Product'", false);
        }

        private static void DynamicQueryParser(
        string serverAddress, 
        string database, 
        string UserId, 
        string Password, 
        string Query, 
        bool outputToCsv)
        {
            string ConnectionString = @"Data Source=" + serverAddress + ";Initial catalog="+ database + ";User ID=" +
                UserId + ";Password=" + Password + ";Persist Security Info=True;Impersonation Level=Impersonate";

            var query = Query;

            Console.WriteLine("About to connect to Analysis Services");
            Console.WriteLine("{0} Date Time \n", DateTime.Now.ToString());

            var adocon = new Microsoft.AnalysisServices.AdomdClient.AdomdConnection();
            adocon.ConnectionString = ConnectionString;
            var table = new System.Data.DataTable();
            adocon.Open();
            Console.WriteLine("Successfully Connected to Analysis Services");
            Console.WriteLine("{0} Date Time \n", DateTime.Now.ToString());

            var adoadapter = new Microsoft.AnalysisServices.AdomdClient.AdomdDataAdapter(query, adocon);
            adoadapter.Fill(table);
            var str = "";
            foreach(System.Data.DataRow row in table.Rows)
            {
                var cnt = row.ItemArray.Count();
                for(int i=0; i < cnt; i++)
                {
                    if (i == (cnt-1))
                    {
                        str += row.ItemArray[i].ToString() + "\n";
                    }
                    else
                    {
                        str += row.ItemArray[i].ToString() + ",";
                    }
                }
            }
            Console.WriteLine("Creating Stream Bytes");
            Console.WriteLine("{0} Date Time \n", DateTime.Now.ToString());

            byte[] bytes = Encoding.ASCII.GetBytes(str);
            Console.WriteLine("Successfully created Stream Bytes");
            Console.WriteLine("{0} Date Time \n", DateTime.Now.ToString());
            var connectionstring = ConfigurationManager.ConnectionStrings["BlobStorage"].ConnectionString;            
            var dest = ConfigurationManager.AppSettings["dest"];
            Task.Run(async () => await SaveTextAsBlobHelper(bytes, connectionstring)).Wait();
            adocon.Close();
            adocon.Dispose();
        }

        private static async Task<int> SaveTextAsBlobHelper(byte[] fileBytes,string connectionstring)
        {


            // Create a BlobServiceClient object which will be used to create a container client
            BlobServiceClient blobServiceClient = new BlobServiceClient(connectionstring);

            //Create a unique name for the container
            string containerName = "powerbiextracteddatatest";
            string fileName = "extractedtable" + Guid.NewGuid().ToString() + ".csv";
            // Create the container and return a container client object
            BlobContainerClient containerClient = await blobServiceClient.CreateBlobContainerAsync(containerName);
            
            BlobClient blobClient = containerClient.GetBlobClient(fileName);
            var stream = new MemoryStream(fileBytes);
            await blobClient.UploadAsync(stream, true);

            return 0;
        }
    }
}

