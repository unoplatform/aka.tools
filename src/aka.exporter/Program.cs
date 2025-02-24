﻿﻿using Azure.Data.Tables;
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
            ?? throw new InvalidOperationException($"Unable to find environment variable AKA_TABLE_CONNECTION_STRING");

        string tableName = "UrlsDetails"; // Replace with your table name
        string outputPath = args.FirstOrDefault()
            ?? throw new InvalidOperationException($"The first parameter must be the output path");

        var client = new TableClient(connectionString, tableName);

        Directory.CreateDirectory(Path.GetDirectoryName(outputPath)!);

        // Store rows in a list first to process formatting
        var rows = new List<(string akaLink, string url, int clicks)>();

        await foreach (var entity in client.QueryAsync<TableEntity>())
        {
            if (!(entity.GetBoolean("IsArchived") ?? true))
            {
                string akaLink = $"https://aka.platform.uno/{entity.GetString("RowKey")}";
                string url = entity.GetString("Url") ?? "";
                int clicks = entity.GetInt32("Clicks") ?? 0;

                rows.Add((akaLink, url, clicks));
            }
        }

        await using (StreamWriter file = new(outputPath))
        {
            // Write the CSV header
            await file.WriteLineAsync("\"AKA Link\",\"Destination URL\",\"Clicks\"");

            // Write each row with proper CSV escaping
            foreach (var (akaLink, url, clicks) in rows)
            {
                string formattedAkaLink = EscapeCsvValue(akaLink);
                string formattedUrl = EscapeCsvValue(url);
                
                await file.WriteLineAsync($"\"{formattedAkaLink}\",\"{formattedUrl}\",{clicks}");
            }
        }

        Console.WriteLine($"CSV data exported to {outputPath}");
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
