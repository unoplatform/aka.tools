using Azure.Data.Tables;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

class Program
{
    static async Task Main(string[] args)
    {
        string connectionString = Environment.GetEnvironmentVariable("AKA_TABLE_CONNECTION_STRING")
                                  ?? throw new InvalidOperationException(
                                      $"Unable to find environment variable AKA_TABLE_CONNECTION_STRING");

        string tableName = "UrlsDetails"; // Replace with your table name
        string outputPath = args.FirstOrDefault()
                            ?? throw new InvalidOperationException($"The first parameter must be the output path");

        var client = new TableClient(connectionString, tableName);

        var directoryName = Path.GetDirectoryName(outputPath);
        if (directoryName is { Length: > 0 })
        {
            Directory.CreateDirectory(directoryName);
        }
        else
        {
            directoryName = ".";
        }

        var fileName = Path.GetFileName(outputPath);
        if (string.IsNullOrEmpty(fileName))
        {
            throw new InvalidOperationException($"The output path must include a file name");
        }

        // Store rows in a list first to process formatting
        var rows = new List<(string akaLink, string url, int clicks, string title, int httpResult, string httpStatusLine)>();

        await foreach (var entity in client.QueryAsync<TableEntity>())
        {
            if (!(entity.GetBoolean("IsArchived") ?? true))
            {
                string akaLink = $"https://aka.platform.uno/{entity.GetString("RowKey")}";
                string url = entity.GetString("Url") ?? "";
                int clicks = entity.GetInt32("Clicks") ?? 0;
                string title = entity.GetString("Title") ?? "";
                
                var (httpResult, httpStatusLine) = await GetHttpResultAndStatusLine(url);

                rows.Add((akaLink, url, clicks, title, httpResult, httpStatusLine));
            }
        }

        var csvFileName =
            Path.Combine(directoryName, Path.GetFileNameWithoutExtension(outputPath) + ".csv");

        await using (StreamWriter file = new(csvFileName))
        {
            // Write the CSV header
            await file.WriteLineAsync("\"AKA Link\",\"Destination URL\",\"Clicks\",\"Title\",\"HTTP Result\",\"HTTP\"");

            // Write each row with proper CSV escaping
            foreach (var (akaLink, url, clicks, title, httpResult, httpStatusLine) in rows)
            {
                string formattedAkaLink = EscapeCsvValue(akaLink);
                string formattedUrl = EscapeCsvValue(url);

                await file.WriteLineAsync($"\"{formattedAkaLink}\",\"{formattedUrl}\",{clicks},\"{EscapeCsvValue(title)}\",{httpResult},\"{EscapeCsvValue(httpStatusLine)}\"");
            }
        }

        Console.WriteLine($"CSV data exported to {outputPath}");
        
        var markdownFileName =
            Path.Combine(directoryName, Path.GetFileNameWithoutExtension(outputPath) + ".md");

        await using (StreamWriter file = new(markdownFileName))
        {
            // Create a beautiful Markdown table
            await file.WriteLineAsync("| AKA Link | Clicks | Title | HTTP Result | HTTP Status Line |");
            await file.WriteLineAsync("| --- | --- | --- | --- | --- |");

            foreach (var (akaLink, url, clicks, title, httpResult, httpStatusLine) in rows)
            {
                string formattedAkaLink = EscapeMarkdownValue(akaLink);
                string formattedUrl = url;
                string formattedTitle = EscapeMarkdownValue(title);
                
                // Coloring the HTTP result based on its value
                string httpResultColor = httpResult switch
                {
                    >= 200 and < 300 => "green",
                    >= 300 and < 400 => "yellow",
                    >= 400 => "red",
                    _ => "gray"
                };
                await file.WriteLineAsync(
                    $"| [{formattedAkaLink}]({formattedUrl}) | {clicks} | {formattedTitle} | <span style=\"color:{httpResultColor}\">{httpResult}</span> | {EscapeMarkdownValue(httpStatusLine)} |");
            }
        }
    }

    private static string EscapeMarkdownValue(string s)
    {
        // Escape pipe characters and backslashes for Markdown tables
        // And html encode everything preventing interpretation as MD/HTML stuff
        return s.Replace("|", "\\|").Replace("\\", "\\\\").Replace("<", "&lt;").Replace(">", "&gt;")
            .Replace("&", "&amp;").Replace("*", "\\*").Replace("_", "\\_").Replace("`", "\\`");
    }

    private static HttpClient _httpClient = new();

    private static async Task<(int httpResult, string httpStatusLine)> GetHttpResultAndStatusLine(string url)
    {
        var timeout = TimeSpan.FromSeconds(8);

        try
        {
            using var cts = new CancellationTokenSource(timeout);
            var response = await _httpClient.GetAsync(url, cts.Token);
            return ((int)response.StatusCode, response.ReasonPhrase ?? "No Status Line");
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Error fetching URL {url}: {ex.Message}");
            return (0, "No Connection");
        }
    }

    // Ensure values with commas or quotes are properly escaped in CSV format
    static string EscapeCsvValue(string value)
    {
        if (value.Contains("\""))
        {
            value = value.Replace("\"", "\"\""); // Escape double quotes
        }

        return value;
    }
}