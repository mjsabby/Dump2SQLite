namespace Dump2SQLite
{
    using System;
    using System.Diagnostics;
    using System.IO;
    using Microsoft.Diagnostics.Runtime;

    internal unsafe static class Program
    {
        private static void Main(string[] args)
        {
            if (args.Length != 1)
            {
                Console.WriteLine("@ Dump2SQLite - Serialize GCHeap to SQLite DB @");
                Console.WriteLine(@"Usage: Dump2SQLite \path\to\filename.dmp");
                Console.WriteLine(@"Output: SQLite DB (\path\to\filename.sqlite)");
                return;
            }

            Stopwatch watch = new Stopwatch();
            watch.Start();

            DataTarget target = DataTarget.LoadCrashDump(args[0]);
            ClrInfo version = target.ClrVersions[0];
            string dacLocation = version.TryGetDacLocation();
            ClrRuntime runtime = target.CreateRuntime(dacLocation);
            
            var heap = runtime.GetHeap();

            if (!heap.CanWalkHeap)
            {
                Console.WriteLine("ERROR: Heap is not walkable. Please collect a new dump, but force a GC prior to taking the dump.");
            }
            
            var fileName = Path.GetFullPath(args[0]).Replace(Path.GetExtension(args[0]), ".sqlite");

            Console.WriteLine("SQLite Database Filename: " + fileName);

            sqlite3* db;
            int error;
            if ((error = NativeMethods.sqlite3_open(fileName, out db)) != 0)
            {
                Console.WriteLine("sqlite3_open failed with error code: " + error);
            }

            CreateTables(db);

            NativeMethods.sqlite3_exec(db, "BEGIN TRANSACTION;", IntPtr.Zero, IntPtr.Zero, IntPtr.Zero);

            sqlite3_stmt* insertObjectsStmt, insertTypesStmt;
            char* tail;

            string insertTypesSql = @"INSERT INTO Types(TypeIndex, MetadataToken, Size, Name) VALUES (@1, @2, @3, @4);";
            NativeMethods.sqlite3_prepare_v2(db, insertTypesSql, insertTypesSql.Length, out insertTypesStmt, out tail);

            string insertObjectsSql = @"INSERT INTO Objects(ObjectId, TypeIndex, Size) VALUES (@1, @2, @3);";
            NativeMethods.sqlite3_prepare_v2(db, insertObjectsSql, insertObjectsSql.Length, out insertObjectsStmt, out tail);

            foreach (ulong obj in heap.EnumerateObjects())
            {
                ClrType type = heap.GetObjectType(obj);
                
                if (type == null)
                {
                    continue;
                }
                
                NativeMethods.sqlite3_bind_int64(insertObjectsStmt, 1, (long)obj);
                NativeMethods.sqlite3_bind_int(insertObjectsStmt, 2, type.Index);
                NativeMethods.sqlite3_bind_int64(insertObjectsStmt, 3, (long)type.GetSize(obj));
                
                NativeMethods.sqlite3_step(insertObjectsStmt);
                NativeMethods.sqlite3_reset(insertObjectsStmt);
            }

            foreach (var type in heap.EnumerateTypes())
            {
                string typeName = type.Name;

                NativeMethods.sqlite3_bind_int(insertTypesStmt, 1, type.Index);
                NativeMethods.sqlite3_bind_int64(insertTypesStmt, 2, type.MetadataToken);
                NativeMethods.sqlite3_bind_int(insertTypesStmt, 3, type.BaseSize);
                NativeMethods.sqlite3_bind_text(insertTypesStmt, 4, typeName, typeName.Length, NativeMethods.Transient);

                NativeMethods.sqlite3_step(insertTypesStmt);
                NativeMethods.sqlite3_reset(insertTypesStmt);
            }

            NativeMethods.sqlite3_finalize(insertTypesStmt);
            NativeMethods.sqlite3_finalize(insertObjectsStmt);

            NativeMethods.sqlite3_exec(db, "END TRANSACTION;", IntPtr.Zero, IntPtr.Zero, IntPtr.Zero);
            NativeMethods.sqlite3_close(db);

            watch.Stop();
            Console.WriteLine("Processing time: {0} milliseconds", watch.ElapsedMilliseconds);
        }

        private static void CreateTables(sqlite3* db)
        {
            sqlite3_stmt* createTableStmt;
            char* tail;

            string createSql = @"CREATE TABLE Objects(ObjectId INTEGER PRIMARY KEY, TypeIndex INTEGER, Size INTEGER);";

            NativeMethods.sqlite3_prepare_v2(db, createSql, createSql.Length, out createTableStmt, out tail);
            NativeMethods.sqlite3_step(createTableStmt);

            createSql = @"CREATE TABLE Types(TypeIndex INTEGER PRIMARY KEY, MetadataToken INTEGER, Size INTEGER, Name TEXT);";

            NativeMethods.sqlite3_prepare_v2(db, createSql, createSql.Length, out createTableStmt, out tail);
            NativeMethods.sqlite3_step(createTableStmt);

            NativeMethods.sqlite3_finalize(createTableStmt);
        }
    }
}