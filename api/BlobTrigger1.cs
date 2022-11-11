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
                            additionalLocationPhrases = row.ChildElements[5] != null ? sst.ChildElements[int.Parse(((Cell)row.ChildElements[5]).CellValue.Text)]?.InnerText : ""
                            
                        };
                        qarecords.Add(record);
                    }

                    try
                    {
                        Uri endpoint = new Uri(endpointURI);
                        AzureKeyCredential credential = new AzureKeyCredential(azureCreds);

                        QuestionAnsweringAuthoringClient client = new QuestionAnsweringAuthoringClient(endpoint, credential);

                        var operation = "add";
                        uint rowcounter = 1;
                        // var additionalPhrasesList = new Dictionary<string, string>();
                        // var additionalphrasses = qarecords.Where(x => x.additionalLocationPhrases != null);
                        // foreach (var additionalPhrase in additionalphrasses)
                        // {
                        //     additionalPhrasesList.Add(additionalPhrase.code, additionalPhrase.additionalLocationPhrases);
                        // }

                        foreach (var record in qarecords)
                        {

                            var rrr = new Dictionary<string, string> { { "test", "test" } };
                            var metadataa = new string[] { "kzn", "durbs", "amanzimtoti" };
                            var request = RequestContent.Create(
                            new[]
                            {
                                    new
                                    {
                                        op=operation,
                                        Value = new
                                        {
                                            questions=new[]
                                            {
                                                $"{record.question}"
                                            },
                                            answer=$"{record.answer}",
                                            metadata=new Dictionary<string, string>
                                            {
                                                {"country",record.country},
                                                {"city",record.city},
                                                {"code",record.code},
                                                {"locphrases" ,record.additionalLocationPhrases}

                                            }
                                        }
                                    }
                            });

                            Operation<Pageable<BinaryData>> updateQnasOperation = client.UpdateQnas(WaitUntil.Started, projectName, request);

                            var qnaOpsResult = updateQnasOperation.Value.ToList();

                            var qnaOpsResultString = Encoding.ASCII.GetString(qnaOpsResult.First());
                            var qnaRoot = JsonConvert.DeserializeObject<Root>(qnaOpsResultString);
                            qarecords[((int)rowcounter)].id = qnaRoot.id.ToString();

                            rowcounter++;
                        }

                    }
                    catch (Exception ex)
                    {

                    }
                }
            }
            else
            {
                log.LogInformation($"C# Blob trigger function blob\n Name:{name} \n not processed due to filename");
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
}



