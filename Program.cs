using System;
using System.IO;
using System.Threading;
using System.Linq;
using System.Diagnostics;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;

namespace FileWatcherToMongoDB
{
    class Program
    {
        static void Main(string[] args)
        {

            string folderPath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "FolderToWatch");
            
            // Check if there are files in the folder to upload before the FileSystemWatcher starts.
            OnStartup(folderPath);


            // Create a FileSystemWatcher to monitor specified folder.
            FileSystemWatcher watcher = new FileSystemWatcher
            {
                Path = folderPath,
                Filter = "*.json"
            };

            // Register a handler that gets called when a file is created.
            watcher.Created += OnCreated;

            // Register a handler that gets callled if the FileSystemWatcher needs to report an error.
            watcher.Error += OnError;

            // Begin watching
            watcher.EnableRaisingEvents = true;

            Console.WriteLine("Press \'Enter\' to quit watching the folder.");
            Console.ReadLine();

        }

        static void OnStartup(string folderPath)
        {
            if (Directory.EnumerateFiles(folderPath, "*.json").Any())
            {
                try
                {
                    Console.WriteLine("Start uploading files left in folder:");
                    string[] files = Directory.GetFiles(folderPath, "*.json");
                    foreach (string file in files)
                    {
                        ProcessFile(file);
                    }
                }
                catch (Exception e)
                {
                    Debug.WriteLine(e.Message);
                }
            }
        }

        static void OnCreated(object source, FileSystemEventArgs e)
        {
            ProcessFile(e.FullPath);
        }

        static bool IsFileUnlocked(string filePath)
        {
            try
            {
                using (File.Open(filePath, FileMode.Open, FileAccess.Read, FileShare.None))
                {
                    return true;
                }
            }
            catch (IOException)
            {
                return false;
            }
        }

        static void OnError(object source, System.IO.ErrorEventArgs e)
        {
            Console.WriteLine("The FileSystemWatcher has detected an error");
            if (e.GetException().GetType() == typeof(InternalBufferOverflowException))
            {
                Console.WriteLine(("The file system watcher experienced an internal buffer overflow: " + e.GetException().Message));
            }
        }

        static string GetActionFromJson(string filePath)
        {
            string action;
            using (StreamReader reader = File.OpenText(filePath))
            {
                JObject jObject = (JObject)JToken.ReadFrom(new JsonTextReader(reader));
                action = (string)jObject["Action"];
            }
            return action;
        }

        static void ProcessFile(string filePath)
        {

            int maximumProcessRetries = 5;
            int delayBeforeRetry = 5000;

            int attempts = 0;

            while (true)
            {
                if (IsFileUnlocked(filePath))
                {
                    // Process the file

                    // Newtonsoft.Json does parse the dates by default, so we have to manually change
                    // the date format to insert it into MongoDB as a date.
                    JsonReader reader = new JsonTextReader(new StringReader(File.ReadAllText(filePath)))
                    {
                        DateParseHandling = DateParseHandling.None
                    };
                    JObject jObject = JObject.Load(reader);

                    JToken tokenCreatedAt = jObject["CreatedAt"];
                    if (tokenCreatedAt != null)
                    {
                        string isoCreatedAt = jObject["CreatedAt"].ToString();
                        jObject["CreatedAt"] = new JRaw($"new ISODate(\"{isoCreatedAt}\")");
                    }
                    
                    try
                    {
                        // MongoDB connection string
                        var connectionString = " ";
                        var client = new MongoClient(connectionString);

                        // Define MongoDB database name
                        var database = client.GetDatabase(" ");
                        var document = BsonSerializer.Deserialize<BsonDocument>(jObject.ToString());
                        var collection = database.GetCollection<BsonDocument>(GetActionFromJson(filePath));
                        collection.InsertOne(document);
                        Console.WriteLine($"{filePath} UPLOADED");
                    }
                    catch (MongoException e)
                    {
                        Console.WriteLine(e.Message);
                        continue;
                    }

                    break;
                }
                attempts += 1;

                if (attempts >= maximumProcessRetries)
                {
                    // Log the error and send out notifications
                    Console.WriteLine("Maximum Process Retries");
                    break;
                }
                Thread.Sleep(delayBeforeRetry);

            }

            // Delete the file after it has been processed
            File.Delete(filePath);
            Console.WriteLine($"{filePath} DELETED");
        }
    }
}
