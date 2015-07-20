namespace Dump2SQLite
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Text;
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

            string dumpLocation = args[0];

            Stopwatch watch = new Stopwatch();
            watch.Start();

            LogInfoWithTimeStamp("Loading Crash Dump from: " + dumpLocation + " ...");

            DataTarget target = DataTarget.LoadCrashDump(dumpLocation);
            ClrInfo version = target.ClrVersions[0]; // TODO: Probably add support for Multiple CLRs or at least warn we're picking the first
            string dacLocation = version.TryGetDacLocation();
            ClrRuntime runtime = target.CreateRuntime(dacLocation);
            
            var heap = runtime.GetHeap();
            if (!heap.CanWalkHeap)
            {
                LogErrorWithTimeStamp("Heap is not walkable. Please collect a new dump.");
                return;
            }

            LogInfoWithTimeStamp("Crash Dump loaded.");

            var perTypeCounts = new Dictionary<int, int>(100000);
            
            var fileName = Path.GetFullPath(args[0]).Replace(Path.GetExtension(args[0]), ".sqlite");

            LogInfoWithTimeStamp("Creating SQLite Database filename: " + fileName + " ...");

            sqlite3* db;
            int error;
            if ((error = NativeMethods.sqlite3_open(fileName, out db)) != 0)
            {
                LogErrorWithTimeStamp("sqlite3_open failed with error code: " + error);
            }

            LogInfoWithTimeStamp("Successfully created SQLite Database filename: " + fileName);

            LogInfoWithTimeStamp("Creating SQLite Tables ...");
            
            if (!CreateTables(db))
            {
                return;
            }

            LogInfoWithTimeStamp("Successfully created SQLite Tables.");

            error = NativeMethods.sqlite3_exec(db, "BEGIN TRANSACTION;", IntPtr.Zero, IntPtr.Zero, IntPtr.Zero);
            if (error != 0)
            {
                LogErrorWithTimeStamp("sqlite3_exec -> BEGIN TRANSACTION; failed to execute with SQLite error code: " + error);
            }

            sqlite3_stmt* insertObjectsStmt,
                insertTypesStmt,
                insertRootsStmt,
                insertBlockingObjectsStmt,
                insertExceptionsStmt,
                insertThreadsStmt;

            if (!PrepareInsertStatement(db, out insertTypesStmt, @"INSERT INTO Types(TypeIndex, Count, Size, Name) VALUES (@1, @2, @3, @4);"))
            {
                return;
            }

            if (!PrepareInsertStatement(db, out insertObjectsStmt, @"INSERT INTO Objects(ObjectId, TypeIndex, Size, ObjRefs) VALUES (@1, @2, @3, @4);"))
            {
                return;
            }

            if (!PrepareInsertStatement(db, out insertRootsStmt, @"INSERT INTO Roots(TypeIndex, ObjectId, Address, AppDomainId, ManagedThreadId, IsInterior, IsPinned, IsPossibleFalsePositive, GCRootKind, Name) VALUES (@1, @2, @3, @4, @5, @6, @7, @8, @9, @10);"))
            {
                return;
            }

            if (!PrepareInsertStatement(db, out insertBlockingObjectsStmt, @"INSERT INTO BlockingObjects(ObjectId, Taken, RecursionCount, Owner, HasSingleOwner, ThreadOwnerIds, ThreadWaiterIds, BlockingReason) VALUES (@1, @2, @3, @4, @5, @6, @7, @8);"))
            {
                return;
            }

            /*
            if (!PrepareInsertStatement(db, out insertExceptionsStmt, @"INSERT INTO Exceptions(ExceptionId, TypeId, Type, Message, Address, InnerExceptionId, HResult, StackId, StackTrace) VALUES (@1, @2, @3, @4, @5, @6, @7, @8, @9);"))
            {
                return;
            }*/

            /*
            if (!PrepareInsertStatement(db, out insertThreadsStmt, @"(GcMode, IsFinalizer, Address, IsAlive, OSThreadId, ManagedThreadId, AppDomain, LockCount, Teb, StackBase, StackLimit, StackId, ExceptionId, IsGC, IsDebuggerHelper, IsThreadpoolTimer, IsThreadpoolCompletionPort, IsThreadpoolWorker, IsThreadpoolWait, IsThreadpoolGate, IsSuspendingEE, IsShutdownHelper, IsAbortRequested, IsAborted, ISGCSuspendPending, IsDebugSuspended, IsBackground, IsUnstarted, IsCoInitialized, IsSTA, IsMTA, BlockingObjects, Roots) VALUES (@1, @2, @3, @4, @5, @6, @7, @8, @9, @10);"))
            {
                return;
            }*/

            LogInfoWithTimeStamp("Starting to populate Objects Table ...");

            var walker = new ReferencesWalker();
            foreach (ulong obj in heap.EnumerateObjects())
            {
                ClrType type = heap.GetObjectType(obj);
                if (type != null)
                {
                    type.EnumerateRefsOfObjectCarefully(obj, walker.Walk);
                    var references = walker.ToString();
                    walker.Clear();

                    int typeIndex = type.Index;
                    if (perTypeCounts.ContainsKey(typeIndex))
                    {
                        ++perTypeCounts[typeIndex];
                    }
                    else
                    {
                        perTypeCounts.Add(typeIndex, 1);
                    }

                    NativeMethods.sqlite3_bind_int64(insertObjectsStmt, 1, (long)obj);
                    NativeMethods.sqlite3_bind_int(insertObjectsStmt, 2, typeIndex);
                    NativeMethods.sqlite3_bind_int(insertObjectsStmt, 3, type.BaseSize);
                    NativeMethods.sqlite3_bind_text(insertObjectsStmt, 4, references, references.Length, NativeMethods.Transient);

                    NativeMethods.sqlite3_step(insertObjectsStmt);
                    NativeMethods.sqlite3_reset(insertObjectsStmt);
                }
            }

            LogInfoWithTimeStamp("Successfully populated Objects Table.");
            LogInfoWithTimeStamp("Starting to populate Types Table ...");

            foreach (var type in heap.EnumerateTypes())
            {
                string typeName = type.Name;
                int typeIndex = type.Index;
                int count;
                if (!perTypeCounts.TryGetValue(typeIndex, out count))
                {
                    count = 0;
                }

                NativeMethods.sqlite3_bind_int(insertTypesStmt, 1, typeIndex);
                NativeMethods.sqlite3_bind_int64(insertTypesStmt, 2, count);
                NativeMethods.sqlite3_bind_int(insertTypesStmt, 3, type.BaseSize);
                NativeMethods.sqlite3_bind_text(insertTypesStmt, 4, typeName, typeName.Length, NativeMethods.Transient);

                NativeMethods.sqlite3_step(insertTypesStmt);
                NativeMethods.sqlite3_reset(insertTypesStmt);
            }

            LogInfoWithTimeStamp("Successfully populated Types Table.");
            LogInfoWithTimeStamp("Starting to populate Roots Table ...");

            foreach (var root in heap.EnumerateRoots())
            {
                NativeMethods.sqlite3_bind_int(insertRootsStmt, 1, root.Type?.Index ?? -1);
                NativeMethods.sqlite3_bind_int64(insertRootsStmt, 2, (long)root.Object);
                NativeMethods.sqlite3_bind_int64(insertRootsStmt, 3, (long)root.Address);
                NativeMethods.sqlite3_bind_int(insertRootsStmt, 4, root.AppDomain?.Id ?? -1);
                NativeMethods.sqlite3_bind_int(insertRootsStmt, 5, root.Thread?.ManagedThreadId ?? -1);
                NativeMethods.sqlite3_bind_int(insertRootsStmt, 6, root.IsInterior ? 1 : 0);
                NativeMethods.sqlite3_bind_int(insertRootsStmt, 7, root.IsPinned ? 1 : 0);
                NativeMethods.sqlite3_bind_int(insertRootsStmt, 8, root.IsPossibleFalsePositive ? 1 : 0);

                string kindString = root.Kind.KindString();
                string rootName = root.Name;

                NativeMethods.sqlite3_bind_text(insertRootsStmt, 9, kindString, kindString.Length, NativeMethods.Transient);
                NativeMethods.sqlite3_bind_text(insertRootsStmt, 10, rootName, rootName.Length, NativeMethods.Transient);

                NativeMethods.sqlite3_step(insertRootsStmt);
                NativeMethods.sqlite3_reset(insertRootsStmt);
            }

            LogInfoWithTimeStamp("Successfully populated Roots Table.");
            LogInfoWithTimeStamp("Starting to populate Blocking Objects Table ...");


            foreach (var blockingObject in heap.EnumerateBlockingObjects())
            {
                NativeMethods.sqlite3_bind_int64(insertBlockingObjectsStmt, 1, (long)blockingObject.Object);
                NativeMethods.sqlite3_bind_int64(insertBlockingObjectsStmt, 2, blockingObject.Taken ? 1 : 0);
                NativeMethods.sqlite3_bind_int64(insertBlockingObjectsStmt, 3, blockingObject.RecursionCount);
                NativeMethods.sqlite3_bind_int64(insertBlockingObjectsStmt, 4, blockingObject.Owner?.ManagedThreadId ?? -1);
                NativeMethods.sqlite3_bind_int(insertBlockingObjectsStmt, 5, blockingObject.HasSingleOwner ? 1 : 0);

                var owners = blockingObject.Owners.Expand();
                NativeMethods.sqlite3_bind_text(insertBlockingObjectsStmt, 6, owners, owners.Length, NativeMethods.Transient);

                var waiters = blockingObject.Waiters.Expand();
                NativeMethods.sqlite3_bind_text(insertBlockingObjectsStmt, 7, waiters, waiters.Length, NativeMethods.Transient);

                var blockingReason = blockingObject.Reason.KindString();
                NativeMethods.sqlite3_bind_text(insertBlockingObjectsStmt, 8, blockingReason, blockingReason.Length, NativeMethods.Transient);

                NativeMethods.sqlite3_step(insertBlockingObjectsStmt);
                NativeMethods.sqlite3_reset(insertBlockingObjectsStmt);
            }

            LogInfoWithTimeStamp("Successfully populated Blocking Objects Table.");

            NativeMethods.sqlite3_finalize(insertTypesStmt);
            NativeMethods.sqlite3_finalize(insertObjectsStmt);
            NativeMethods.sqlite3_finalize(insertRootsStmt);
            NativeMethods.sqlite3_finalize(insertBlockingObjectsStmt);

            error = NativeMethods.sqlite3_exec(db, "END TRANSACTION;", IntPtr.Zero, IntPtr.Zero, IntPtr.Zero);
            if (error != 0)
            {
                LogErrorWithTimeStamp("sqlite3_exec -> END TRANSACTION; failed to execute with SQLite error code: " + error);
            }

            NativeMethods.sqlite3_close(db);

            watch.Stop();
            LogInfoWithTimeStamp("Processing time: " + watch.ElapsedMilliseconds + "  milliseconds");
        }

        private static string Expand(this IList<ClrThread> threads)
        {
            if (threads == null)
            {
                return "-1";
            }

            string threadList;

            var count = threads.Count;
            if (count > 0)
            {
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < count; ++i)
                {
                    if (threads[i] != null)
                    {
                        sb.Append(threads[i].ManagedThreadId);

                        if (count - i != 1)
                        {
                            sb.Append(',');
                        }
                    }
                }

                threadList = sb.ToString();
            }
            else
            {
                threadList = "-1";
            }

            return threadList;
        }

        private static bool PrepareInsertStatement(sqlite3* db, out sqlite3_stmt* stmt, string sql)
        {
            char* tail;
            int error = NativeMethods.sqlite3_prepare_v2(db, sql, sql.Length, out stmt, out tail);

            if (error != 0)
            {
                LogErrorWithTimeStamp("sqlite3_prepare_v2 -> " + sql + " failed to execute with SQLite error code: " + error);
                return false;
            }

            return true;
        }

        private static bool CreateTable(sqlite3* db, string sql)
        {
            sqlite3_stmt* createTableStmt;
            char* tail;
            
            int error = NativeMethods.sqlite3_prepare_v2(db, sql, sql.Length, out createTableStmt, out tail);

            if (error != 0)
            {
                LogErrorWithTimeStamp("sqlite3_prepare_v2 -> " + sql + " failed to execute with SQLite error code: " + error);
                NativeMethods.sqlite3_finalize(createTableStmt);
                return false;
            }

            error = NativeMethods.sqlite3_step(createTableStmt);

            if (error != 101)
            {
                LogErrorWithTimeStamp("sqlite3_step -> " + sql + " failed to execute with SQLite error code: " + error);
                NativeMethods.sqlite3_finalize(createTableStmt);
                return false;
            }

            NativeMethods.sqlite3_finalize(createTableStmt);
            return true;
        }

        private static string KindString(this GCRootKind kind)
        {
            switch (kind)
            {
                case GCRootKind.StaticVar:
                    return "StaticVar";
                case GCRootKind.Finalizer:
                    return "Finalizer";
                case GCRootKind.Pinning:
                    return "Pinning";
                case GCRootKind.AsyncPinning:
                    return "AsyncPinning";
                case GCRootKind.LocalVar:
                    return "LocalVar";
                case GCRootKind.ThreadStaticVar:
                    return "ThreadStaticVar";
                case GCRootKind.Strong:
                    return "Strong";
                case GCRootKind.Weak:
                    return "Weak";
                default:
                    return "Unknown";
            }
        }

        private static string KindString(this BlockingReason kind)
        {
            switch (kind)
            {
                case BlockingReason.None:
                    return "None";
                case BlockingReason.Unknown:
                    return "Unknown";
                case BlockingReason.Monitor:
                    return "Monitor";
                case BlockingReason.MonitorWait:
                    return "MonitorWait";
                case BlockingReason.WaitOne:
                    return "WaitOne";
                case BlockingReason.WaitAll:
                    return "WaitAll";
                case BlockingReason.WaitAny:
                    return "WaitAny";
                case BlockingReason.ThreadJoin:
                    return "ThreadJoin";
                case BlockingReason.ReaderAcquired:
                    return "ReaderAcquired";
                case BlockingReason.WriterAcquired:
                    return "WriterAcquired";
                default:
                    return "Unknown";
            }
        }

        private static bool CreateTables(sqlite3* db)
        {
            if (!CreateTable(db, @"CREATE TABLE Objects(ObjectId INTEGER PRIMARY KEY, TypeIndex INTEGER, Size INTEGER, ObjRefs TEXT);"))
            {
                return false;
            }

            if (!CreateTable(db, @"CREATE TABLE Types(TypeIndex INTEGER PRIMARY KEY, Count INTEGER, Size INTEGER, Name TEXT);"))
            {
                return false;
            }

            if (!CreateTable(db, "CREATE TABLE Roots(TypeIndex INTEGER, ObjectId INTEGER, Address INTEGER, AppDomainId INTEGER, ManagedThreadId INTEGER, IsInterior BOOLEAN, IsPinned BOOLEAN, IsPossibleFalsePositive BOOLEAN, GCRootKind TEXT, Name TEXT);"))
            {
                return false;
            }

            if (!CreateTable(db, "CREATE TABLE BlockingObjects(ObjectId INTEGER, Taken BOOLEAN, RecursionCount INTEGER, Owner INTEGER, HasSingleOwner BOOL, ThreadOwnerIds TEXT, ThreadWaiterIds TEXT, BlockingReason TEXT);"))
            {
                return false;
            }

            return true;
        }
        
        private static void LogErrorWithTimeStamp(string message)
        {
            Console.WriteLine("[ERROR][{0}]: {1}", DateTime.Now, message);
        }

        private static void LogInfoWithTimeStamp(string message)
        {
            Console.WriteLine("[INFO][{0}]: {1}", DateTime.Now, message);
        }

        private sealed class ReferencesWalker
        {
            private readonly List<ulong> References = new List<ulong>(100);

            private readonly StringBuilder sb = new StringBuilder(1024 * 10);

            public void Walk(ulong @object, int fieldOffset)
            {
                this.References.Add(@object);
            }

            public void Clear()
            {
                this.References.Clear();
                this.sb.Clear();
            }

            public override string ToString()
            {
                int count = this.References.Count;
                for (int i = 0; i < count; ++i)
                {
                    this.sb.Append(this.References[i]);

                    if (count - i != 1)
                    {
                        this.sb.Append(',');
                    }
                }

                return this.sb.ToString();
            }
        }
    }
}