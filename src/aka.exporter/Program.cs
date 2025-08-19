using Azure.Data.Tables;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using Polly;
using Polly.Extensions.Http;

class Program
{
    static async Task Main(string[] args)
    {
        var connectionString = Environment.GetEnvironmentVariable("AKA_TABLE_CONNECTION_STRING")
                               ?? throw new InvalidOperationException(
                                   $"Unable to find environment variable AKA_TABLE_CONNECTION_STRING");

        var tableName = "UrlsDetails"; // Replace with your table name
        var outputPath = args.FirstOrDefault()
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
        var bag = new ConcurrentBag<(string rowKey, string akaLink, string url, int clicks, string title, int httpResult, string httpStatusLine)>();

        var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = 12 };

        await Parallel.ForEachAsync(client.QueryAsync<TableEntity>(), parallelOptions,
            async (entity, ct) =>
            {
                if (!(entity.GetBoolean("IsArchived") ?? true))
                {
                    var rowKey = entity.GetString("RowKey") ?? "";
                    var akaLink = $"https://aka.platform.uno/{rowKey}";
                    var url = entity.GetString("Url") ?? "";
                    var clicks = entity.GetInt32("Clicks") ?? 0;
                    var title = entity.GetString("Title") ?? "";

                    var (httpResult, httpStatusLine) = await GetHttpResultAndStatusLine(url, ct);

                    bag.Add((rowKey, akaLink, url, clicks, title, httpResult, httpStatusLine));
                }
            });

        var csvFileName =
            Path.Combine(directoryName, Path.GetFileNameWithoutExtension(outputPath) + ".csv");
        
        var rows = bag
            .OrderByDescending(row => row.httpResult)
            .ThenBy(row => row.akaLink)
            .ToArray();

        await using (StreamWriter file = new(csvFileName))
        {
            // Write the CSV header
            await file.WriteLineAsync("\"AKA Link\",\"Destination URL\",\"Clicks\",\"Title\",\"HTTP Result\",\"HTTP\"");

            // Write each row with proper CSV escaping
            foreach (var (rowKey, akaLink, url, clicks, title, httpResult, httpStatusLine) in rows)
            {
                string formattedAkaLink = EscapeCsvValue(akaLink);
                string formattedUrl = EscapeCsvValue(url);

                await file.WriteLineAsync($"\"{formattedAkaLink}\",\"{formattedUrl}\",{clicks},\"{EscapeCsvValue(title)}\",{httpResult},\"{EscapeCsvValue(httpStatusLine)}\"");
            }
        }

        Console.WriteLine($"CSV data exported to {outputPath}");
        
        // Calculate summary statistics
        var successCount = rows.Count(row => row.httpResult >= 200 && row.httpResult < 300);
        var redirectCount = rows.Count(row => row.httpResult >= 300 && row.httpResult < 400);
        var errorCount = rows.Count(row => row.httpResult >= 400);
        var notFoundCount = rows.Count(row => row.httpResult == 0);
        
        // Output summary for GitHub workflow to capture
        var summary = $"{successCount} Oks, {errorCount} Errors, {notFoundCount} NotFound, {redirectCount} Redirects";
        Console.WriteLine($"EXPORT_SUMMARY={summary}");
        
        var markdownFileName =
            Path.Combine(directoryName, Path.GetFileNameWithoutExtension(outputPath) + ".md");

        await using (StreamWriter file = new(markdownFileName))
        {
            // Create a beautiful Markdown table
            await file.WriteLineAsync("| AKA Link | Title | HTTP Result | HTTP Status Line |");
            await file.WriteLineAsync("| --- | --- | --- | --- |");

            foreach (var (rowKey, akaLink, url, clicks, title, httpResult, httpStatusLine) in rows)
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
        Timeout = TimeSpan.FromSeconds(30) // Increased timeout to account for retries
    };

    // Retry policy for handling transient failures
    private static readonly IAsyncPolicy<HttpResponseMessage> _retryPolicy = Policy
        .Handle<HttpRequestException>()
        .Or<TaskCanceledException>()
        .Or<SocketException>()
        .Or<TimeoutException>()
        .OrResult<HttpResponseMessage>(r => !r.IsSuccessStatusCode && (
            r.StatusCode == HttpStatusCode.RequestTimeout ||
            r.StatusCode == HttpStatusCode.TooManyRequests ||
            r.StatusCode == HttpStatusCode.InternalServerError ||
            r.StatusCode == HttpStatusCode.BadGateway ||
            r.StatusCode == HttpStatusCode.ServiceUnavailable ||
            r.StatusCode == HttpStatusCode.GatewayTimeout))
        .WaitAndRetryAsync(
            retryCount: 3,
            sleepDurationProvider: retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)) + TimeSpan.FromMilliseconds(Random.Shared.Next(0, 1000)),
            onRetry: (outcome, timespan, retryCount, context) =>
            {
                var url = context.ContainsKey("url") ? context["url"].ToString() : "unknown";
                Console.WriteLine($"[RETRY {retryCount}/3] {url} - waiting {timespan.TotalMilliseconds:F0}ms - reason: {outcome.Exception?.Message ?? outcome.Result?.StatusCode.ToString()}");
            });

    private static async Task<(int httpResult, string httpStatusLine)> GetHttpResultAndStatusLine(string url, CancellationToken ct)
    {
        var timeout = TimeSpan.FromSeconds(10);

        try
        {
            var context = new Context { ["url"] = url };
            
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(timeout);
            
            var response = await _retryPolicy.ExecuteAsync(async (ctx, cancellationToken) =>
            {
                return await _httpClient.GetAsync(url, cancellationToken);
            }, context, cts.Token);

            var statusCode = (int)response.StatusCode;
            var reasonPhrase = response.ReasonPhrase ?? "No Status Line";
            
            // Log error messages for HTTP error status codes
            if (statusCode >= 400)
            {
                Console.Error.WriteLine($"ERROR: HTTP {statusCode} for URL {url}: {reasonPhrase}");
            }

            return (statusCode, reasonPhrase);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            Console.Error.WriteLine($"ERROR: Operation was cancelled for URL {url}");
            return (0, "Operation Cancelled");
        }
        catch (OperationCanceledException)
        {
            Console.Error.WriteLine($"ERROR: Timeout after 3 retries for URL {url}");
            return (0, "Timeout");
        }
        catch (HttpRequestException ex)
        {
            Console.Error.WriteLine($"ERROR: HTTP error after 3 retries for URL {url}: {ex.Message}");
            return (0, "HTTP Error");
        }
        catch (SocketException ex)
        {
            Console.Error.WriteLine($"ERROR: DNS/Socket error after 3 retries for URL {url}: {ex.Message}");
            return (0, "DNS/Socket Error");
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"ERROR: Unexpected error after 3 retries for URL {url}: {ex.Message}");
            return (0, "Unexpected Error");
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