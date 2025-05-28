using Azure.Data.Tables;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
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
        var bag = new ConcurrentBag<(string akaLink, string url, int clicks, string title, int httpResult, string httpStatusLine)>();

        var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = 12 };

        await Parallel.ForEachAsync(client.QueryAsync<TableEntity>(), parallelOptions,
            async (entity, ct) =>
            {
                if (!(entity.GetBoolean("IsArchived") ?? true))
                {
                    string akaLink = $"https://aka.platform.uno/{entity.GetString("RowKey")}";
                    string url = entity.GetString("Url") ?? "";
                    int clicks = entity.GetInt32("Clicks") ?? 0;
                    string title = entity.GetString("Title") ?? "";

                    var (httpResult, httpStatusLine) = await GetHttpResultAndStatusLine(url, ct);

                    bag.Add((akaLink, url, clicks, title, httpResult, httpStatusLine));
                }
            });

        var csvFileName =
            Path.Combine(directoryName, Path.GetFileNameWithoutExtension(outputPath) + ".csv");
        
        var rows = bag.ToArray();

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
            await file.WriteLineAsync("| AKA Link | Title | HTTP Result | HTTP Status Line |");
            await file.WriteLineAsync("| --- | --- | --- | --- |");

            foreach (var (akaLink, url, clicks, title, httpResult, httpStatusLine) in rows)
            {
                string formattedAkaLink = EscapeMarkdownValue(akaLink);
                string formattedUrl = url;
                string formattedTitle = EscapeMarkdownValue(title);
                
                // Coloring the HTTP result based on its value
                string httpResultBadge = httpResult switch
                {
                    >= 200 and < 300 => $"![](https://img.shields.io/badge/{httpResult}-success-green)",
                    >= 300 and < 400 => $"![](https://img.shields.io/badge/{httpResult}-redirect-yellow)",
                    >= 400 => $"![](https://img.shields.io/badge/{httpResult}-error-red)",
                    _ => $"![](https://img.shields.io/badge/{httpResult}-unknown-gray)"
                };
                await file.WriteLineAsync(
                    $"| [{formattedAkaLink}]({formattedUrl}) | {formattedTitle} | {httpResultBadge} | {EscapeMarkdownValue(httpStatusLine)} |");
            }
        }
    }

    private static string EscapeMarkdownValue(string s)
    {
        var sb = new StringBuilder(s.Length);
        foreach (var c in s)
        {
            sb.Append(c switch
            {
                '|' => "\\|",
                '\\' => "\\\\",
                '<' => "&lt;",
                '>' => "&gt;",
                '&' => "&amp;",
                '*' => "\\*",
                '_' => "\\_",
                '`' => "\\`",
                _ => c.ToString()
            });
        }
        return sb.ToString();
    }

    private static readonly HttpClient _httpClient = new HttpClient
    {
        Timeout = TimeSpan.FromSeconds(10)
    };

    private static async Task<(int httpResult, string httpStatusLine)> GetHttpResultAndStatusLine(string url, CancellationToken ct)
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