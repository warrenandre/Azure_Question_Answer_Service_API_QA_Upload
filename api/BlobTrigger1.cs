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
                    var qarecords = new List<Record>();
                    foreach (var row in rows.Skip(1))
                    {
                        var tquestion = sst.ChildElements[int.Parse(((Cell)row.ChildElements[0]).CellValue.Text)].InnerText;
                        var record = new Record
                        {
                            question = sst.ChildElements[int.Parse(((Cell)row.ChildElements[0]).CellValue.Text)].InnerText,
                            answer = sst.ChildElements[int.Parse(((Cell)row.ChildElements[1]).CellValue.Text)].InnerText,
                            country = sst.ChildElements[int.Parse(((Cell)row.ChildElements[2]).CellValue.Text)].InnerText,
                            city = sst.ChildElements[int.Parse(((Cell)row.ChildElements[3]).CellValue.Text)].InnerText,
                            code = sst.ChildElements[int.Parse(((Cell)row.ChildElements[4]).CellValue.Text)].InnerText,
                            additionalPhrases = sst.ChildElements[int.Parse(((Cell)row.ChildElements[4]).CellValue.Text)].ToString().Split(";")
                        };
                        qarecords.Add(record);
                    }

                    try
                    {
                        Uri endpoint = new Uri("https://qnaanna.cognitiveservices.azure.com/");
                        AzureKeyCredential credential = new AzureKeyCredential("0f7a4abb027549959f5b1e44bd92a03d");

                        QuestionAnsweringAuthoringClient client = new QuestionAnsweringAuthoringClient(endpoint, credential);
                        foreach (var record in qarecords)
                        {
                            var request = RequestContent.Create(
                            new[]{
                            new
                            {
                                op="replace",
                                Value = new
                                {
                                    questions=new[]
                                    {
                                        $"{record.question}"
                                    },
                                    answer=$"{record.answer}",
                                    metadata=new Dictionary<string,string>{
                                        {"country",record.country},
                                        {"city",record.city},
                                        {"code",record.code}
                                        }
                                }
                            }
                              });
                            var result = client.UpdateQnas(WaitUntil.Completed, "annabot", request);
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

    }

    public class Record
    {
        public string question { get; set; }
        public string answer { get; set; }
        public string country { get; set; }
        public string city { get; set; }
        public string code { get; set; }
        public Array additionalPhrases { get; set; }
    }
}
