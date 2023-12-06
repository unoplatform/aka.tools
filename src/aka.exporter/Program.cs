using Azure.Data.Tables;
using System;
using System.IO;
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

        await using (StreamWriter file = new(outputPath))
        {
            await foreach (var entity in client.QueryAsync<TableEntity>())
            {
                if (!(entity.GetBoolean("IsArchived") ?? true))
                {
                    // You can customize the output format here
                    await file.WriteLineAsync($"https://aka.platform.uno/{entity.GetString("RowKey")} ; {entity.GetString("Url")}"); // Replace with your column name
                }
            }
        }

        Console.WriteLine($"Data exported to {outputPath}");
    }
}
