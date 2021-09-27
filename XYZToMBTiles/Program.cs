using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using Dapper.Contrib.Extensions;
using Microsoft.Data.Sqlite;

namespace XYZToMBTiles
{
    class Program
    {
        private static string baseDirectory = "/Volumes/Overland NZ/tiles/";

        static void Main(string[] args)
        {
            int minZoom = 5;
            int maxZoom = 15;

            string[] zoomLevels = Directory.GetDirectories(baseDirectory);

            Parallel.ForEach(zoomLevels, zoomLevel =>
            {
                if (int.TryParse(zoomLevel.Split("/").Last(), out int level))
                {
                    if (level >= minZoom && level <= maxZoom)
                    {
                        using (SqliteConnection connection = new SqliteConnection($"Filename=/Volumes/Overland NZ/nz-{level}.mbtiles"))
                        {
                            connection.Execute("CREATE TABLE metadata (name text, value text);");
                            connection.Execute("CREATE TABLE tiles (zoom_level integer, tile_column integer, tile_row integer, tile_data blob);");
                            connection.Execute("CREATE UNIQUE INDEX tile_index on tiles (zoom_level, tile_column, tile_row);");

                            string[] columns = Directory.GetDirectories(Path.Join(baseDirectory, level.ToString()));

                            foreach (string columnPath in columns)
                            {
                                if (int.TryParse(columnPath.Split("/").Last(), out int column))
                                {
                                    string[] rows = Directory.GetFiles(Path.Join(baseDirectory, level.ToString(), column.ToString()));

                                    foreach (var rowPath in rows)
                                    {
                                        if (rowPath.EndsWith(".png"))
                                        {
                                            var t = new tile
                                            {
                                                tile_column = column,
                                                tile_row = int.Parse(rowPath.Split("/").Last().Split(".")[0]),
                                                zoom_level = level,
                                                tile_data = File.ReadAllBytes(rowPath)
                                            };

                                            connection.Insert(t);
                                            Console.WriteLine($"{Thread.CurrentThread.ManagedThreadId} Written {level} - {column} - {rowPath}");
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            });
        }
    }

    class tile
    {
        public int zoom_level { get; set; }
        public int tile_column { get; set; }
        public int tile_row { get; set; }
        public byte[] tile_data { get; set; }
    }
}