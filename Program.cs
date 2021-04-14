using System;

namespace DynamoDBViaLowLevelInterface
{
	using Amazon.DynamoDBv2;
	using Amazon.DynamoDBv2.DataModel;
	using Amazon.DynamoDBv2.DocumentModel;
	using Amazon.DynamoDBv2.Model;
	using System.Collections.Generic;
	using System.Threading;
	using System.Threading.Tasks;
	//using AWSSDK;  // Installing AWSSDK created duplicate definitions

	class Program
	{
		const string MovieTableName = "OldMovies";
		static void Main(string[] args)
		{
			Console.WriteLine("Choose option:");
			Console.WriteLine("1 - to create OldMovies table");
			Console.WriteLine("2 - to insert 3 movies into OldMovies table");
			string choice = Console.ReadLine();
			int intResult = 0;
			Int32.TryParse(choice, out intResult);
			switch (intResult)
			{
				case 1: 
					CreateOldMoviesTableAsync(); 
					break;
				case 2: 
					InsertSampleEntriesAsync(); 
					break;
				default:
					Console.WriteLine("Invalid Entry. Please choose and integer between 1 and 2.");
					break;
			}

		}  // This is end of main

		private static void CreateOldMoviesTableAsync()
		{
			AmazonDynamoDBClient client = new AmazonDynamoDBClient();

			string tableToCreate = MovieTableName;

			var tables = client.ListTablesAsync();
			var tableNames = tables.Result.TableNames;
			bool tableFound = false;
			foreach(string tableName in tableNames)
			{
				if(tableName == tableToCreate)
				{
					tableFound = true;
					break;
				}
			}
			if(tableFound)
			{
				Console.WriteLine("Could not create table.  Table " + tableToCreate + " already exists");
				return;
			}

			// Create the table

			var request = new CreateTableRequest
			{
				TableName = MovieTableName,
				AttributeDefinitions = new List<AttributeDefinition>()
				{
					new AttributeDefinition
					{
						AttributeName = "MovieName",
						AttributeType = "S",
					}
				},
				KeySchema = new List<KeySchemaElement>()
				{
					new KeySchemaElement
					{
						AttributeName = "MovieName",
						KeyType = "HASH"  // Partition key
					}
				},
				ProvisionedThroughput = new ProvisionedThroughput
				{
					ReadCapacityUnits = 10,
					WriteCapacityUnits = 5
				}
			};

			client.CreateTableAsync(request);

			// Wait 5 seconds for table creation to complete
			System.Threading.Thread.Sleep(TimeSpan.FromSeconds(5));

			Console.WriteLine("Table " + MovieTableName + " created");

		}
		
		private static void InsertSampleEntriesAsync()
		{

			AmazonDynamoDBClient client = new AmazonDynamoDBClient();

			string status = "";

			try
			{
				var res = client.DescribeTableAsync(MovieTableName);
				status = res.Result.Table.TableStatus;
			}
			catch(NotImplementedException)
			{
				status = "not ACTIVE";
			}

			if (status != "ACTIVE")
			{
				Console.WriteLine("Table " + MovieTableName + " is not yet available.  Please try again.");
				return;
			}
			

			var request1 = new PutItemRequest
			{
				TableName = MovieTableName,
				Item = new Dictionary<string, AttributeValue>
				{
					{ "MovieName", new AttributeValue {S = "North By Northwest"} },
					{ "Stars", new AttributeValue {S = "Cary Grant, Eva Marie Saint, James Mason"} },
					{ "YearMade", new AttributeValue { N = "1959"} }
				}
			};

			var request2 = new PutItemRequest
			{
				TableName = MovieTableName,
				Item = new Dictionary<string, AttributeValue>
				{
					{"MovieName", new AttributeValue {S = "The Maltese Falcon"} },
					{"Stars", new AttributeValue {S= "Humphrey Bogart, Mary Astor" } },
					{ "YearMade", new AttributeValue { N = "1941"} }
				}
			};

			var request3 = new PutItemRequest
			{
				TableName = MovieTableName,
				Item = new Dictionary<string, AttributeValue>
				{
					{"MovieName", new AttributeValue { S = "Attack of the Killer Tomatoes!"} },
					{"Stars", new AttributeValue { S = "David Miller, George Wilson, Sharon Tayler" } },
					{"YearMade", new AttributeValue { N = "1978"} }
				}
			};

			client.PutItemAsync(request1);
			System.Threading.Thread.Sleep(TimeSpan.FromSeconds(5));
			client.PutItemAsync(request2);
			System.Threading.Thread.Sleep(TimeSpan.FromSeconds(5));
			client.PutItemAsync(request3);
			System.Threading.Thread.Sleep(TimeSpan.FromSeconds(5));

			Console.WriteLine("Movies added");
		}

	}
}
