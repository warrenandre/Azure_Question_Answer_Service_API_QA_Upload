using System;
using System.IO;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml.Spreadsheet;
using System.Linq;
using System.Collections.Generic;
using Azure.Core;
using Azure.AI.Language.QuestionAnswering;
using Azure;
using Azure.AI.Language.QuestionAnswering.Authoring;
using System.Text;
using Newtonsoft.Json;
using DocumentFormat.OpenXml;

namespace Company.Function
{
    public class BlobTrigger1
    {
        [FunctionName("BlobTrigger1")]
        public void Run([BlobTrigger("qafile/{name}", Connection = "qna8844_STORAGE")] Stream myBlob, string name, ILogger log)
        {
            log.LogInformation($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes");
            if (name == "QATemplate.xlsx")
            {
                var projectName = Environment.GetEnvironmentVariable("project_name");
                var endpointURI = Environment.GetEnvironmentVariable("endpointURI");
                var azureCreds = Environment.GetEnvironmentVariable("AzureKeyCredential");
                using (SpreadsheetDocument doc = SpreadsheetDocument.Open(myBlob, false))
                {
                    WorkbookPart workbookPart = doc.WorkbookPart;
                    SharedStringTablePart sstpart = workbookPart.GetPartsOfType<SharedStringTablePart>().First();
                    SharedStringTable sst = sstpart.SharedStringTable;

                    WorksheetPart worksheetPart = workbookPart.WorksheetParts.First();
                    Worksheet sheet = worksheetPart.Worksheet;

                    var cells = sheet.Descendants<Cell>();
                    var rows = sheet.Descendants<Row>();

                    log.LogInformation(string.Format("Row count = {0}", rows.LongCount()));
                    log.LogInformation(string.Format("Cell count = {0}", cells.LongCount()));
                    var qarecords = new List<QnaPair>();

                    foreach (var row in rows.Skip(1))
                    {
                        var tquestion = sst.ChildElements[int.Parse(((Cell)row.ChildElements[0]).CellValue.Text)]?.InnerText;
                        var record = new QnaPair
                        {
                            question = sst.ChildElements[int.Parse(((Cell)row.ChildElements[0]).CellValue.Text)]?.InnerText,
                            answer = sst.ChildElements[int.Parse(((Cell)row.ChildElements[1]).CellValue.Text)]?.InnerText,
                            country = sst.ChildElements[int.Parse(((Cell)row.ChildElements[2]).CellValue.Text)]?.InnerText,
                            city = sst.ChildElements[int.Parse(((Cell)row.ChildElements[3]).CellValue.Text)]?.InnerText,
                            code = sst.ChildElements[int.Parse(((Cell)row.ChildElements[4]).CellValue.Text)]?.InnerText,
                            additionalLocationPhrases = sst.ChildElements[int.Parse(((Cell)row.ChildElements[5]).CellValue.Text)]?.InnerText

                        };
                        qarecords.Add(record);
                    }

                    try
                    {
                        Uri endpoint = new Uri(endpointURI);
                        AzureKeyCredential credential = new AzureKeyCredential(azureCreds);
                        //https://learn.microsoft.com/en-us/azure/cognitive-services/language-service/question-answering/how-to/authoring

                        QuestionAnsweringAuthoringClient client = new QuestionAnsweringAuthoringClient(endpoint, credential);

                        var operation = "add";
                        
                        
                        while (qarecords.Any())
                        {
                            int rowcounter = 0;
                            int takecount = qarecords.Count >= 1000 ? 1000:qarecords.Count;
                            object[] arr = new object[takecount];
                            foreach (var record in qarecords.Take(takecount))
                            {

                                var alternativeQ = string.Concat(record.question," ", record.code);
                                var alternativeQ2 = string.Concat(record.question," ", record.city);
                                
                                arr[rowcounter] = new
                                {
                                    op = operation,
                                    Value = new
                                    {
                                        questions = new[]
                                                {
                                                $"{record.question}",
                                                $"{alternativeQ}",
                                                $"{alternativeQ2}"
                                            },
                                        answer = $"{record.answer}",
                                        metadata = new Dictionary<string, string>
                                            {
                                                {"country",record.country},
                                                {"city",record.city},
                                                {"code",record.code},
                                                {"locphrases" ,record.additionalLocationPhrases}

                                            }
                                    }
                                };
                                rowcounter++;

                            }
                            qarecords= qarecords.Skip(takecount).ToList();
                            var request = RequestContent.Create(
                                                    arr);

                        var updateQnasOperation = client.UpdateQnas(WaitUntil.Completed, projectName, request);
                        log.LogInformation(string.Format("Rows uploaded = {0}", takecount));
                        }
                        

                    }
                    catch (Exception ex)
                    {
                        log.LogError(ex,ex.Message);
                    }
                }
            }
            else
            {
                log.LogInformation($"C# Blob trigger function blob\n Name:{name} \n not processed due to filename");
            }

            
        }

    }



    public class QnaPair
    {
        public string id { get; set; }
        public string question { get; set; }
        public string answer { get; set; }
        public string country { get; set; }
        public string city { get; set; }
        public string code { get; set; }
        public string additionalLocationPhrases { get; set; }
    }

    public class Dialog
    {
        public bool isContextOnly { get; set; }
        public List<object> prompts { get; set; }
    }

    public class Metadata
    {
        public string country { get; set; }
        public string city { get; set; }
        public string code { get; set; }
        public string system_metadata_qna_edited_manually { get; set; }
    }

    public class Root
    {
        public int id { get; set; }
        public string answer { get; set; }
        public string source { get; set; }
        public List<string> questions { get; set; }
        public Metadata metadata { get; set; }
        public Dialog dialog { get; set; }
        public List<object> activeLearningSuggestions { get; set; }
        public bool isDocumentText { get; set; }
        public string lastUpdatedDateTime { get; set; }
    }

}




