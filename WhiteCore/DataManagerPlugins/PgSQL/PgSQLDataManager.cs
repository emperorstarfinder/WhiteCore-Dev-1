/*
 * Copyright (c) Contributors, http://whitecore-sim.org/
 * See CONTRIBUTORS.TXT for a full list of copyright holders.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the WhiteCore-Sim Project nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE DEVELOPERS ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE CONTRIBUTORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using Npgsql;
using WhiteCore.DataManager.Migration;
using WhiteCore.Framework.ConsoleFramework;
using WhiteCore.Framework.Services;
using WhiteCore.Framework.Utilities;

namespace WhiteCore.DataManager.PgSQL
{
    public class PgSQLDataLoader : DataManagerBase
    {
        string m_connectionString = "";

        public override string Identifier
        {
            get { return "PgSQLData"; }
        }

        #region Database

        public override void ConnectToDatabase (string connectionString, string migratorName, bool validateTables)
        {
            m_connectionString = connectionString;
            var c = new NpgsqlConnection (connectionString);
            InitializeMonoSecurity();

            int subStrA = connectionString.IndexOf ("Database=", StringComparison.Ordinal);
            int subStrB = connectionString.IndexOf (";", subStrA, StringComparison.Ordinal);
            string noDatabaseConnector = m_connectionString.Substring (0, subStrA) +
                                         m_connectionString.Substring (subStrB + 1);

            retry:
            try
            {
                var param = new Dictionary<string, object> ();
                param.Add ("WITH OWNER", "postgres");
                param.Add ("ENCODING", "UTF8");
                param.Add ("LC_COLLATE", "en_US.UTF-8");
                param.Add ("LC_CTYPE", "en_US.UTF-8");

                ExecuteNonQuery (noDatabaseConnector, "create schema IF NOT EXISTS " + c.Database, param, false);
            } catch
            {
                MainConsole.Instance.Error (
                    "[PySQLDatabase]: We cannot connect to the PySQL instance you have provided.\n" +
                    "Please make sure it is online, and then press enter to try again.");
                Console.Read ();
                goto retry;
            }

            var migrationManager = new MigrationManager (this, migratorName, validateTables);
            migrationManager.DetermineOperation ();
            migrationManager.ExecuteOperation ();
        }

        public void InitializeMonoSecurity ()
        {
            if (Util.IsLinux)
            {

                if (AppDomain.CurrentDomain.GetData ("MonoSecurityPostgresAdded") == null)
                {
                    AppDomain.CurrentDomain.SetData ("MonoSecurityPostgresAdded", "true");

                    AppDomain currentDomain = AppDomain.CurrentDomain;
                    currentDomain.AssemblyResolve += ResolveEventHandlerMonoSec;
                }
            }
        }

        static Assembly ResolveEventHandlerMonoSec (object sender, ResolveEventArgs args)
        {
            Assembly MyAssembly = null;

            if (args.Name.Substring (0, args.Name.IndexOf (",", StringComparison.Ordinal)) == "Mono.Security")
            {
                MyAssembly = Assembly.LoadFrom ("lib/Mono.Security.dll");
            }

            //Return the loaded assembly.
            return MyAssembly;
        }

        public void CloseDatabase (NpgsqlConnection connection)
        {
            //Interlocked.Decrement (ref m_locked);
            connection.Close ();
            //connection.Dispose();
        }

        public override void CloseDatabase (DataReaderConnection connection)
        {
            if (connection != null && connection.DataReader != null)
                connection.DataReader.Close ();
            //Interlocked.Decrement (ref m_locked);
            //m_connection.Close();
            //m_connection.Dispose();
        }

        #endregion

        #region Query
        /*NpgsqlConnection dbquerycon;
        protected NpgsqlCommand PrepReader(string sql)
        {
            try
            {
                NpgsqlConnection connection = new NpgsqlConnection(m_connectionString);
                connection.Open();
                var cmd = new NpgsqlCommand (sql, connection);

                return cmd;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return null;
        }

        protected void CloseQueryConnection()
        {
            dbquerycon.Close();
        }
        */
        public NpgsqlCommand Query (string sql)
        {
            return Query( sql, new Dictionary<string, object> ());
        }


        public NpgsqlCommand Query (string sql, Dictionary<string, object> parameters)
        {
            try
            {
                //var qrycmd = PrepReader(sql);
                var dbcon = new NpgsqlConnection (m_connectionString);
                dbcon.Open ();
                var cmd = new NpgsqlCommand (sql, dbcon);

                foreach (KeyValuePair<string, object> p in parameters)
                     cmd.Parameters.AddWithValue (p.Key, p.Value);
 
        //*        NpgsqlDataReader dr = cmd.ExecuteReader ();
                //dbcon.Close ();

       //*         return dr;  
             
                return cmd;
            } catch (Exception e)
            {
                MainConsole.Instance.Error ("[PgSQLDataLoader] Query(" + sql + "), " + e);
                return null;
            }
        }

        public int ExecuteNonQuery (string sql)
        {
            return ExecuteNonQuery (m_connectionString, sql, new Dictionary<string, object>(), true);
        }

        public int ExecuteNonQuery (string sql, Dictionary<string, object> parameters)
        {
            return ExecuteNonQuery (m_connectionString, sql, parameters, true);
        }

        public int ExecuteNonQuery (string connStr, string sql, Dictionary<string, object> parameters)
        {
            return ExecuteNonQuery (connStr, sql, parameters, true);
        }

        public int ExecuteNonQuery (string connStr, string sql, Dictionary<string, object> parameters, bool spamConsole)
        {
            int retVal = 0;
            try
            {
                var dbcon = new NpgsqlConnection (connStr);
                dbcon.Open ();
                var cmd = new NpgsqlCommand (sql, dbcon);

                foreach (KeyValuePair<string, object> p in parameters)
                {
                    cmd.Parameters.AddWithValue (p.Key, p.Value);
                }
                // Execute command
                retVal = cmd.ExecuteNonQuery ();

                dbcon.Close ();
            } catch (Exception e)

            {
                if (spamConsole)
                    MainConsole.Instance.ErrorFormat ("[PgSQLDataLoader] ExecuteNonQuery({0}), {1}", sql, e);
                else
                    throw e;
            }
            return retVal;
        }

        public override List<string> QueryFullData (string whereClause, string table, string wantedValue)
        {
            string query = String.Format ("select {0} from {1} {2}", wantedValue, table, whereClause);
            return QueryFullData2 (query);
        }

        public override List<string> QueryFullData (string whereClause, QueryTables tables, string wantedValue)
        {
            string query = string.Format ("SELECT {0} FROM {1} {2}", wantedValue, tables.ToSQL (), whereClause);
            return QueryFullData2 (query);
        }

        List<string> QueryFullData2 (string query)
        {
//*            var dbcon = new NpgsqlConnection (m_connectionString);
//*            dbcon.Open ();
            //NpgsqlCommand command = new NpgsqlCommand (query, dbcon);

            IDataReader reader;
            var retVal = new List<string> ();
            try
            {
                var cmd = Query (query, new Dictionary<string, object> ());
//                using (reader = Query (query, new Dictionary<string, object> ()))
                using (reader = cmd.ExecuteReader())
                {
                    while (reader.Read ())
                    {
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            retVal.Add (reader [i].ToString ());
                        }
                    }
//*                    dbcon.Close ();
                    cmd.Connection.Close();
                    return retVal;
                }
            } catch (Exception e)
            {
                MainConsole.Instance.Error ("[PgSQLDataLoader] QueryFullData(" + query + "), " + e);
                return null;
            }
        }

        public override DataReaderConnection QueryData (string whereClause, string table, string wantedValue)
        {
            string query = String.Format ("select {0} from {1} {2}", wantedValue, table, whereClause);
            var cmd = QueryData2 (query);
            var data = cmd.ExecuteReader();    
            //            return new DataReaderConnection { DataReader = QueryData2 (query) };
            return new DataReaderConnection { DataReader = data, Connection = cmd.Connection };
        }

        public override DataReaderConnection QueryData (string whereClause, QueryTables tables, string wantedValue)
        {
            string query = string.Format ("SELECT {0} FROM {1} {2}", wantedValue, tables.ToSQL (), whereClause);
            var cmd = QueryData2 (query);
            var data = cmd.ExecuteReader();    
            return new DataReaderConnection { DataReader = data, Connection = cmd.Connection };
//            return new DataReaderConnection { DataReader = QueryData2 (query) };
        }

        //IDataReader QueryData2 (string query)
        NpgsqlCommand QueryData2 (string query)
        {
            return Query (query, new Dictionary<string, object> ());
        }

        public override List<string> Query (string[] wantedValue, string table, QueryFilter queryFilter,
                                           Dictionary<string, bool> sort, uint? start, uint? count)
        {
            string query = string.Format ("SELECT {0} FROM {1}", string.Join (", ", wantedValue), table);
            return Query2 (query, queryFilter, sort, start, count);
        }

        public override List<string> Query (string[] wantedValue, QueryTables tables, QueryFilter queryFilter,
                                           Dictionary<string, bool> sort, uint? start, uint? count)
        {
            string query = string.Format ("SELECT {0} FROM {1}", string.Join (", ", wantedValue), tables.ToSQL ());
            return Query2 (query, queryFilter, sort, start, count);
        }

        List<string> Query2 (string sqll, QueryFilter queryFilter, Dictionary<string, bool> sort, uint? start,
                            uint? count)
        {
            string query = sqll;
            var ps = new Dictionary<string, object> ();
            var retVal = new List<string> ();
            var parts = new List<string> ();

            if (queryFilter != null && queryFilter.Count > 0)
            {
                query += " WHERE " + queryFilter.ToSQL (':', out ps);
            }

            if (sort != null && sort.Count > 0)
            {
                parts = new List<string> ();
                foreach (KeyValuePair<string, bool> sortOrder in sort)
                {
                    parts.Add (string.Format ("`{0}` {1}", sortOrder.Key, sortOrder.Value ? "ASC" : "DESC"));
                }
                query += " ORDER BY " + string.Join (", ", parts.ToArray ());
            }

            if (start.HasValue)
            {
                query += " LIMIT " + start.Value;
                if (count.HasValue)
                {
                    query += ", " + count.Value;
                }
            }

//*            var dbcon = new NpgsqlConnection (m_connectionString);
//*            dbcon.Open ();
            //NpgsqlCommand command = new NpgsqlCommand (query, dbcon);

            IDataReader reader;
            int i = 0;
            try
            {
                var cmd = Query (query, ps);
//                using (reader = Query (query, ps))
                using (reader = cmd.ExecuteReader())
                {
                    while (reader.Read ())
                    {
                        for (i = 0; i < reader.FieldCount; i++)
                        {
                            Type r = reader [i].GetType ();
                            retVal.Add (r == typeof(DBNull) ? null : reader [i].ToString ());
                        }
                    }
//*                    dbcon.Close ();
                    cmd.Connection.Close();
                    return retVal;
                }
            } catch (Exception e)
            {
                MainConsole.Instance.Error ("[PgSQLDataLoader] Query(" + query + "), " + e);
                return null;
            }
        }

        /*public override Dictionary<string, List<string>> QueryNames(string[] wantedValue, string table, QueryFilter queryFilter, Dictionary<string, bool> sort, uint? start, uint? count)
        {
        }*/

        public override Dictionary<string, List<string>> QueryNames (string[] keyRow, object[] keyValue, string table,
                                                                    string wantedValue)
        {
            string query = String.Format ("select {0} from {1} where ", wantedValue, table);
            return QueryNames2 (keyRow, keyValue, query);
        }

        public override Dictionary<string, List<string>> QueryNames (string[] keyRow, object[] keyValue,
                                                                    QueryTables tables, string wantedValue)
        {
            string query = string.Format ("SELECT {0} FROM {1} where ", wantedValue, tables.ToSQL ());
            return QueryNames2 (keyRow, keyValue, query);
        }

        Dictionary<string, List<string>> QueryNames2 (IList<string> keyRow, object[] keyValue, string query)
        {

            var retVal = new Dictionary<string, List<string>> ();
            var ps = new Dictionary<string, object> ();

            int i = 0;
            foreach (object value in keyValue)
            {
                query += String.Format ("{0} = :{1} and ", keyRow [i], keyRow [i]);
                ps [":" + keyRow [i]] = value;
                i++;
            }
            query = query.Remove (query.Length - 5);

            var dbcon = new NpgsqlConnection (m_connectionString);
            dbcon.Open ();
            //var command = new NpgsqlCommand (query, dbcon);

            IDataReader reader;
            try
            {
                var cmd = Query (query, ps);
                using (reader = cmd.ExecuteReader())
//                using (reader = Query (query, ps))
                {
                    while (reader.Read ())
                    {
                        for (i = 0; i < reader.FieldCount; i++)
                        {
                            Type r = reader [i].GetType ();
                            AddValueToList (ref retVal, reader.GetName (i), r == typeof(DBNull) ? null : reader [i].ToString ());
                        }
                    }
                    //dbcon.Close ();
                    cmd.Connection.Close();
                    return retVal;
                }
            } catch (Exception e)
            {
                MainConsole.Instance.Error ("[PgSQLDataLoader] QueryNames(" + query + "), " + e);
                return null;
            }
        }

        static void AddValueToList (ref Dictionary<string, List<string>> dic, string key, string value)
        {
            if (!dic.ContainsKey (key))
            {
                dic.Add (key, new List<string> ());
            }

            dic [key].Add (value);
        }

        #endregion

        #region Update

        public override bool Update (string table, Dictionary<string, object> values,
                                    Dictionary<string, int> incrementValue, QueryFilter queryFilter, uint? start,
                                    uint? count)
        {
            if ((values == null || values.Count < 1) && (incrementValue == null || incrementValue.Count < 1))
            {
                MainConsole.Instance.Warn ("Update attempted with no values");
                return false;
            }

            string query = string.Format ("UPDATE {0}", table);
            var ps = new Dictionary<string, object> ();

            string filter = "";
            if (queryFilter != null && queryFilter.Count > 0)
            {
                filter = " WHERE " + queryFilter.ToSQL (':', out ps);
            }

            var parts = new List<string> ();
            if (values != null)
            {
                foreach (KeyValuePair<string, object> value in values)
                {
                    string key = ":updateSet_" + value.Key.Replace ("`", "");
                    ps [key] = value.Value;
                    parts.Add (string.Format ("{0} = {1}", value.Key, key));
                }
            }
            if (incrementValue != null)
            {
                foreach (KeyValuePair<string, int> value in incrementValue)
                {
                    string key = ":updateSet_increment_" + value.Key.Replace ("`", "");
                    ps [key] = value.Value;
                    parts.Add (string.Format ("{0} = {0} + {1}", value.Key, key));
                }
            }

            query += " SET " + string.Join (", ", parts.ToArray ()) + filter;

            if (start.HasValue)
            {
                query += " LIMIT " + start.Value;
                if (count.HasValue)
                {
                    query += ", " + count.Value;
                }
            }

            try
            {
                ExecuteNonQuery (query, ps);
            } catch (NpgsqlException e)
            {
                MainConsole.Instance.Error ("[PgSQLDataLoader] Update(" + query + "), " + e);
            }
            return true;
        }

        #endregion

        #region Insert

        public override bool InsertMultiple (string table, List<object[]> values)
        {
            string query = String.Format ("insert into {0} select ", table);
            var parameters = new Dictionary<string, object> ();
            int i = 0;

            foreach (object[] value in values)
            {
                foreach (object v in value)
                {
                    parameters [Util.ConvertDecString (i)] = v;
                    query += ":" + Util.ConvertDecString (i++) + ",";
                }
                query = query.Remove (query.Length - 1);
                query += " union all select ";
            }
            query = query.Remove (query.Length - (" union all select ").Length);

            try
            {
                ExecuteNonQuery (query, parameters);
            } catch (Exception e)
            {
                MainConsole.Instance.Error ("[PgSQLDataLoader] Insert(" + query + "), " + e);
            }
            return true;
        }

        public override bool Insert (string table, object[] values)
        {
            string query = String.Format ("insert into {0} values (", table);
            var parameters = new Dictionary<string, object> ();
            int i = 0;

            foreach (object o in values)
            {
                parameters [Util.ConvertDecString (i)] = o;
                query += ":" + Util.ConvertDecString (i++) + ",";
            }
            query = query.Remove (query.Length - 1);
            query += ")";

            try
            {
                ExecuteNonQuery (query, parameters);
            } catch (Exception e)
            {
                MainConsole.Instance.Error ("[PgSQLDataLoader] Insert(" + query + "), " + e);
            }
            return true;
        }

        bool InsertOrReplace (string table, Dictionary<string, object> row, bool insert)
        {
            string query = (insert ? "INSERT" : "REPLACE") + " INTO " + table +
                " (" + string.Join (", ", row.Keys.ToArray<string> ()) + ")";
            var ps = new Dictionary<string, object> ();

            foreach (KeyValuePair<string, object> field in row)
            {
                string key = ":" +
                             field.Key.Replace ("`", "")
                                  .Replace ("(", "_")
                                  .Replace (")", "")
                                  .Replace (" ", "_")
                                  .Replace ("-", "minus")
                                  .Replace ("+", "add")
                                  .Replace ("/", "divide")
                                  .Replace ("*", "multiply");
                ps [key] = field.Value;
            }
            query += " VALUES( " + string.Join (", ", ps.Keys.ToArray<string> ()) + " )";

            try
            {
                ExecuteNonQuery (query, ps);
            } catch (Exception e)
            {
                MainConsole.Instance.Error ("[PgSQLDataLoader] " + (insert ? "Insert" : "Replace") + "(" + query + "), " +
                e);
            }
            return true;
        }

        public override bool Insert (string table, Dictionary<string, object> row)
        {
            return InsertOrReplace (table, row, true);
        }

        public override bool Insert (string table, object[] values, string updateKey, object updateValue)
        {
            string query = String.Format ("insert into {0} VALUES(", table);
            var param = new Dictionary<string, object> ();
            int i = 0;

            foreach (object o in values)
            {
                param ["?" + Util.ConvertDecString (i)] = o;
                query += "?" + Util.ConvertDecString (i++) + ",";
            }
            param [":update"] = updateValue;
            query = query.Remove (query.Length - 1);
            query += String.Format (") ON DUPLICATE KEY UPDATE {0} = :update", updateKey);    // TODO:  THIS WILL BREAK!!!
            try
            {
                ExecuteNonQuery (query, param);
            } catch (Exception e)
            {
                MainConsole.Instance.Error ("[PgSQLDataLoader] Insert(" + query + "), " + e);
                return false;
            }
            return true;
        }

        public override bool InsertSelect (string tableA, string[] fieldsA, string tableB, string[] valuesB)
        {
            string query = string.Format ("INSERT INTO {0}{1} SELECT {2} FROM {3}",
                               tableA,
                               (fieldsA.Length > 0 ? " (" + string.Join (", ", fieldsA) + ")" : ""),
                               string.Join (", ", valuesB),
                               tableB
                           );

            try
            {
                ExecuteNonQuery (query, new Dictionary<string, object> (0));
            } catch (Exception e)
            {
                MainConsole.Instance.Error ("[PgSQLDataLoader] INSERT .. SELECT (" + query + "), " + e);
            }
            return true;
        }

        #endregion

        #region REPLACE INTO

        public override bool Replace (string table, Dictionary<string, object> row)
        {
            return InsertOrReplace (table, row, false);
        }

        #endregion

        #region Delete

        public override bool DeleteByTime (string table, string key)
        {
            var filter = new QueryFilter ();
            filter.andLessThanEqFilters ["(UNIX_TIMESTAMP(`" + key.Replace ("`", "") + "`) - UNIX_TIMESTAMP())"] = 0;

            return Delete (table, filter);
        }

        public override bool Delete (string table, QueryFilter queryFilter)
        {
            var ps = new Dictionary<string, object> ();
            string query = "DELETE FROM " + table +
                (queryFilter != null ? (" WHERE " + queryFilter.ToSQL (':', out ps)) : "");

            try
            {
                ExecuteNonQuery (query, ps);
            } catch (Exception e)
            {
                MainConsole.Instance.Error ("[PgSQLDataLoader] Delete(" + query + "), " + e);
                return false;
            }
            return true;
        }

        #endregion

        public override string ConCat (string[] toConcat)
        {
            string returnValue = toConcat.Aggregate ("concat(", (current, s) => current + (s + ","));
            return returnValue.Substring (0, returnValue.Length - 1) + ")";
        }

        #region Tables

        public override void CreateTable (string table, ColumnDefinition[] columns, IndexDefinition[] indexDefinitions)
        {
            table = table.ToLower ();
            if (TableExists (table))
            {
                throw new DataManagerException ("Trying to create a table with name of one that already exists.");
            }

            IndexDefinition primary = null;
            foreach (IndexDefinition index in indexDefinitions)
            {
                if (index.Type == IndexType.Primary)
                {
                    primary = index;
                    break;
                }
            }

            var columnDefinition = new List<string> ();

            foreach (ColumnDefinition column in columns)
            {
//                columnDefinition.Add ("`" + column.Name + "` TYPE  " + GetColumnTypeStringSymbol (column.Type));
                columnDefinition.Add (column.Name + " " + GetColumnTypeStringSymbol (column.Type));

            }
            if (primary != null && primary.Fields.Length > 0)
            {
//                columnDefinition.Add ("PRIMARY KEY (`" + string.Join ("`, `", primary.Fields) + "`)");
                columnDefinition.Add ("PRIMARY KEY (" + string.Join (", ", primary.Fields) + ")");
            }



            var indicesQuery = new List<string> (indexDefinitions.Length);
            var indicesExtra = new List<string> ();
            string indicesUnique = "";

            // add any unique or normal index
            foreach (IndexDefinition index in indexDefinitions)
            {
                string type = "";
                switch (index.Type)
                {
                case IndexType.Primary:
                    continue;               // added above
                case IndexType.Unique:
                    type = "UNIQUE";
                    //break;
                    indicesExtra.Add ("CREATE UNIQUE INDEX idx_" + string.Join ("_", index.Fields) + " ON " + table + "(" +
                        string.Join (", ", index.Fields) + ")");
                    continue;
                default:
                    indicesExtra.Add ("CREATE INDEX idx_" + string.Join ("_", index.Fields) + " ON " + table + "(" +
                    string.Join (", ", index.Fields) + ")");
                    continue;
                }

/*                if (index.IndexSize == 0)
                    //indicesQuery.Add (string.Format ("{0}( {1} )", type, "`" + string.Join ("`, `", index.Fields) + "`"));
                    //indicesQuery.Add (string.Format ("{0}( {1} )", type, string.Join (", ", index.Fields)));
                    indicesUnique = string.Format ("{0}( {1} )", type, string.Join (", ", index.Fields));
                else
                    //indicesQuery.Add (string.Format ("{0}( {1} )", type, "`" + string.Join ("`, `", index.Fields) + "`" + "(" + index.IndexSize + ")"));
                    //indicesQuery.Add (string.Format ("{0}( {1} )", type, string.Join (", ", index.Fields) + "(" + index.IndexSize + ")"));
                    indicesUnique = string.Format ("{0}( {1} )", type, string.Join (", ", index.Fields) + "(" + index.IndexSize + ")");
*/
            }

            // the main table/index creation (primary index only)
            string query = string.Format ("CREATE TABLE " + table + " ( {0})",
                string.Join (", ", columnDefinition.ToArray ()) );

            //string query = string.Format ("CREATE TABLE " + table + " ( {0} {1})",
            //                       string.Join (", ", columnDefinition.ToArray ()),
            //    indicesUnique == "" ? string.Empty: ", " + indicesUnique);
            
//                string query = string.Format ("CREATE TABLE " + table + " ( {0} {1})",
//                    string.Join (", ", columnDefinition.ToArray ()),
//                    indicesQuery.Count > 0
//                    ? ", " + string.Join (", ", indicesQuery[0]) //.ToArray ())    Only a single index allowed on creation of table

            // need to create additional indexes using the CREATE INDEX command
            // eg CREATE INDEX idx_flags_scope ON gridregions( Flags, ScopeID );
            try
            {
                ExecuteNonQuery (query);
            } catch (Exception e)
            {
                MainConsole.Instance.ErrorFormat ("[PgSQLDataLoader] CreateTable: {0}", e);
            }

            // additional index's?
            if (indicesExtra.Count > 0)
            {
                foreach (string idxquery in indicesExtra)
                {
                    try
                    {
                        ExecuteNonQuery (idxquery);
                    } catch (Exception e)
                    {
                        MainConsole.Instance.ErrorFormat ("[PgSQLDataLoader] Create index: {0}", e);
                    }
                }
            }
        }

        public override void UpdateTable (string table, ColumnDefinition[] columns, IndexDefinition[] indexDefinitions,
                                         Dictionary<string, string> renameColumns)
        {
            table = table.ToLower ();
            if (!TableExists (table))
            {
                throw new DataManagerException ("Trying to update a table with name of one that does not exist.");
            }

            List<ColumnDefinition> oldColumns = ExtractColumnsFromTable (table);

            var removedColumns = new Dictionary<string, ColumnDefinition> ();
            var modifiedColumns = new Dictionary<string, ColumnDefinition> ();

            Dictionary<string, ColumnDefinition> addedColumns =
                columns.Where (column => !oldColumns.Contains (column)).ToDictionary (column => column.Name.ToLower ());
            foreach (ColumnDefinition column in oldColumns.Where(column => !columns.Contains(column)))
            {
                if (addedColumns.ContainsKey (column.Name.ToLower ()))
                {
                    if (column.Name.ToLower () != addedColumns [column.Name.ToLower ()].Name.ToLower () ||
                        column.Type != addedColumns [column.Name.ToLower ()].Type)
                    {
                        modifiedColumns.Add (column.Name.ToLower (), addedColumns [column.Name.ToLower ()]);
                    }
                    addedColumns.Remove (column.Name.ToLower ());
                } else
                {
                    removedColumns.Add (column.Name.ToLower (), column);
                }
            }

            try
            {
                foreach (
                    string query in
                        addedColumns.Values.Select(
                            column => "add '" + column.Name + "' " + GetColumnTypeStringSymbol(column.Type) +
                                      " ")
                                    .Select(
                                        addedColumnsQuery =>
                                        string.Format("alter table " + table + " " + addedColumnsQuery)))
                {
                    ExecuteNonQuery (query, new Dictionary<string, object> ());
                }
                foreach (
                    string query in modifiedColumns.Values.Select(
                        column => "rename column '" + column.Name + "' " + GetColumnTypeStringSymbol(column.Type) + " ")
                    .Select(
                        modifiedColumnsQuery => string.Format("alter table " + table + " " + modifiedColumnsQuery)))
                {
                    ExecuteNonQuery (query, new Dictionary<string, object> ());
                }
                foreach (
                    string query in
                        removedColumns.Values.Select(column => "drop '" + column.Name + "' ")
                                      .Select(
                                          droppedColumnsQuery => string.Format("alter table " + table + " " + droppedColumnsQuery)))
                {
                    ExecuteNonQuery (query, new Dictionary<string, object> ());
                }
            } catch (Exception e)
            {
                MainConsole.Instance.ErrorFormat ("[PgSQLDataLoader] UpdateTable: {0}", e);
            }

            Dictionary<string, IndexDefinition> oldIndicesDict = ExtractIndicesFromTable (table);

            var removeIndices = new List<string> ();
            var oldIndexNames = new List<string> (oldIndicesDict.Count);
            var oldIndices = new List<IndexDefinition> (oldIndicesDict.Count);
            var newIndices = new List<IndexDefinition> ();

            foreach (KeyValuePair<string, IndexDefinition> oldIndex in oldIndicesDict)
            {
                oldIndexNames.Add (oldIndex.Key);
                oldIndices.Add (oldIndex.Value);
            }
            int i = 0;
            foreach (IndexDefinition oldIndex in oldIndices)
            {
                bool found = false;
                foreach (IndexDefinition newIndex in indexDefinitions)
                {
                    if (oldIndex.Equals (newIndex))
                    {
                        found = true;
                        break;
                    }
                }
                if (!found)
                {
                    removeIndices.Add (oldIndexNames [i]);
                }
                ++i;
            }

            foreach (IndexDefinition newIndex in indexDefinitions)
            {
                bool found = false;
                foreach (IndexDefinition oldIndex in oldIndices)
                {
                    if (oldIndex.Equals (newIndex))
                    {
                        found = true;
                        break;
                    }
                }
                if (!found)
                {
                    newIndices.Add (newIndex);
                }
            }

            foreach (string oldIndex in removeIndices)
            {
                ExecuteNonQuery (string.Format ("ALTER TABLE {0} DROP INDEX {1}", table, oldIndex),
                    new Dictionary<string, object> ());
            }
            foreach (IndexDefinition newIndex in newIndices)
            {
                ExecuteNonQuery (
                    string.Format ("ALTER TABLE {0} ADD {1} ({2})", table,
                        newIndex.Type == IndexType.Primary
                                      ? "PRIMARY KEY"
                                      : (newIndex.Type == IndexType.Unique ? "UNIQUE" : "INDEX"),
                        string.Join (", ", newIndex.Fields)), new Dictionary<string, object> ());
            }
        }

        public override string GetColumnTypeStringSymbol (ColumnTypes type)
        {
            switch (type)
            {
            case ColumnTypes.Double:
                return "DOUBLE PRECISION";
            case ColumnTypes.Integer11:
                return "INTEGER";       // need to process unsigned // (11)";
            case ColumnTypes.Integer30:
                return "BIGINT";       // need to process unsigned // (30)";
            case ColumnTypes.UInteger11:
                return "INTEGER";       // need to process unsigned // (11) UNSIGNED";
            case ColumnTypes.UInteger30:
                return "BIGINT";       // need to process unsigned // (30) UNSIGNED";
            case ColumnTypes.Char40:
                return "CHAR(40)";
            case ColumnTypes.Char39:
                return "CHAR(39)";
            case ColumnTypes.Char38:
                return "CHAR(38)";
            case ColumnTypes.Char37:
                return "CHAR(37)";
            case ColumnTypes.Char36:
                return "CHAR(36)";
            case ColumnTypes.Char35:
                return "CHAR(35)";
            case ColumnTypes.Char34:
                return "CHAR(34)";
            case ColumnTypes.Char33:
                return "CHAR(33)";
            case ColumnTypes.Char32:
                return "CHAR(32)";
            case ColumnTypes.Char5:
                return "CHAR(5)";
            case ColumnTypes.Char1:
                return "CHAR(1)";
            case ColumnTypes.Char2:
                return "CHAR(2)";
            case ColumnTypes.String:
                return "TEXT";
            case ColumnTypes.String10:
                return "VARCHAR(10)";
            case ColumnTypes.String16:
                return "VARCHAR(16)";
            case ColumnTypes.String30:
                return "VARCHAR(30)";
            case ColumnTypes.String32:
                return "VARCHAR(32)";
            case ColumnTypes.String36:
                return "VARCHAR(36)";
            case ColumnTypes.String45:
                return "VARCHAR(45)";
            case ColumnTypes.String50:
                return "VARCHAR(50)";
            case ColumnTypes.String64:
                return "VARCHAR(64)";
            case ColumnTypes.String128:
                return "VARCHAR(128)";
            case ColumnTypes.String100:
                return "VARCHAR(100)";
            case ColumnTypes.String255:
                return "VARCHAR(255)";
            case ColumnTypes.String512:
                return "VARCHAR(512)";
            case ColumnTypes.String1024:
                return "VARCHAR(1024)";
            case ColumnTypes.String8196:
                return "VARCHAR(8196)";
            case ColumnTypes.Text:
                return "TEXT";
            case ColumnTypes.MediumText:
                return "TEXT";
            case ColumnTypes.LongText:
                return "TEXT";
            case ColumnTypes.Blob:
                return "BLOB";
            case ColumnTypes.LongBlob:
                return "BLOB";
            case ColumnTypes.Date:
                return "DATE";
            case ColumnTypes.DateTime:
                return "TIMESTAMP";
            case ColumnTypes.Float:
                return "REAL";
            case ColumnTypes.TinyInt1:
                return "SMALLINT";
            case ColumnTypes.TinyInt4:
                return "INTEGER";
            case ColumnTypes.UTinyInt4:
                return "INTEGER";    // need to process unsigned // UNSIGNED";
            case ColumnTypes.Binary32:
                return "BINARY(32)";
            case ColumnTypes.Binary64:
                return "BINARY(64)";
            case ColumnTypes.UUID:
                return "CHAR(36)";
            default:
                throw new DataManagerException ("Unknown column type.");
            }
        }

        public override string GetColumnTypeStringSymbol (ColumnTypeDef coldef)
        {
            string symbol;
            switch (coldef.Type)
            {
            case ColumnType.Blob:
            case ColumnType.LongBlob:
                symbol = "BYTEA";
                break;
            case ColumnType.Boolean:
                symbol = "BOOL";
                break;
            case ColumnType.Char:
                symbol = "CHAR(" + coldef.Size + ")";
                break;
            case ColumnType.Date:
                symbol = "DATE";
                break;
            case ColumnType.DateTime:
                symbol = "TIMESTAMP";
                break;
            case ColumnType.Double:
                symbol = "DOUBLE PRECISION";
                break;
            case ColumnType.Float:
                symbol = "REAL";
                break;
            case ColumnType.Integer:
                symbol = "INT";  //  no size //(" + coldef.Size + ")"; //   Need to process for unsgned // + (coldef.unsigned ? " unsigned" : "");
                break;
            case ColumnType.TinyInt:
                symbol = "SMALLINT";     // no size //(" + coldef.Size + ")";    // need to process for unsigned // + (coldef.unsigned ? " unsigned" : "");
                break;
            case ColumnType.String:
                symbol = "VARCHAR(" + coldef.Size + ")";
                break;
            case ColumnType.Text:
            case ColumnType.MediumText:
            case ColumnType.LongText:
                symbol = "TEXT";
                break;
            case ColumnType.UUID:
                symbol = "CHAR(36)";
                break;
            case ColumnType.Binary:
                symbol = "BYTEA";       // no size //NARY(" + coldef.Size + ")";
                break;
            default:
                throw new DataManagerException ("Unknown column type.");
            }

            // special cases for autoimcrement fields
            if (coldef.Type == ColumnType.Integer && coldef.auto_increment)
                symbol = "BIGSERIAL";
            if (coldef.Type == ColumnType.TinyInt && coldef.auto_increment)
                symbol = "SERIAL";

            return symbol + (coldef.isNull ? " NULL" : " NOT NULL") +
            ((coldef.isNull && coldef.defaultValue == null)
                        ? " DEFAULT NULL"
                    : (coldef.defaultValue != null ? " DEFAULT " + coldef.defaultValue.MySqlEscape () : "")); // +
// see above                   ((coldef.Type == ColumnType.Integer || coldef.Type == ColumnType.TinyInt) && coldef.auto_increment
//                        ? " AUTO_INCREMENT"
//                        : "");
        }

        public override void DropTable (string tableName)
        {
            tableName = tableName.ToLower ();
            try
            {
                ExecuteNonQuery (string.Format ("drop table {0};", tableName), new Dictionary<string, object> ());
            } catch (Exception e)
            {
                MainConsole.Instance.ErrorFormat ("[PgSQLDataLoader] DropTable {0}", e);
            }
        }

        public override void ForceRenameTable (string oldTableName, string newTableName)
        {
            newTableName = newTableName.ToLower ();
            try
            {
                ExecuteNonQuery (string.Format ("RENAME TABLE {0} TO {1};", oldTableName, newTableName),
                    new Dictionary<string, object> ());
            } catch (Exception e)
            {
                MainConsole.Instance.ErrorFormat ("[PgSQLDataLoader] ForceRenameTable {0}", e);
            }
        }

        protected override void CopyAllDataBetweenMatchingTables (string sourceTableName, string destinationTableName,
                                                                 ColumnDefinition[] columnDefinitions,
                                                                 IndexDefinition[] indexDefinitions)
        {
            sourceTableName = sourceTableName.ToLower ();
            destinationTableName = destinationTableName.ToLower ();
            try
            {
                ExecuteNonQuery (
                    string.Format ("insert into {0} select * from {1};", destinationTableName, sourceTableName),
                    new Dictionary<string, object> ());
            } catch (Exception e)
            {
                MainConsole.Instance.ErrorFormat ("[PgSQLDataLoader] CopyAllDataBetweenMatchingTables {0}", e);
            }
        }

        public override bool TableExists (string table)
        {
            var qry = string.Format ("SELECT * FROM information_schema.tables WHERE table_schema = '{0}' AND table_name = '{1}';",
                "whitecore", table);

            //var ret = ExecuteNonQuery (qry, new Dictionary<string, object> ());
            // returns -1 if table does not exist


            IDataReader rdr;
            var retVal = new List<string> ();
            //bool retVal = false;
            try
            {
                var cmd = Query (qry);
//                rdr = Query (qry);
                rdr = cmd.ExecuteReader();
                {
            //        reader.Read();
            //        retVal = (reader.FieldCount > 1);
                    while (rdr.Read ())
                    {
                        for (int i = 0; i < rdr.FieldCount; i++)
                        {
//                            retVal = (reader.RecordsAffected > 0);
                            retVal.Add (rdr [i].ToString ().ToLower ());
                        }

                    }

                }
                // tidy up
                if (rdr != null)
                    rdr.Close ();
                cmd.Connection.Close();

            } catch (Exception e)
            {
                MainConsole.Instance.ErrorFormat ("[PgSQLDataLoader] TableExists: {0}", e);
            }


            return retVal.Contains (table.ToLower ());
            //return retVal;
        }

        internal string ConvertPgTypeToColumnDef(string pgFieldType, string colsize)
        {
            if (colsize == "")
                colsize = "0";
            
            // convert expanded type definitions to standard column types
            if (pgFieldType == "smallint")
                return "tinyint(2)";

            if (pgFieldType == "character varying")
                return "varchar(" + colsize + ")";

            if (pgFieldType == "character")
                return "char(" + colsize + ")";

            if (pgFieldType == "bpchar")
                return "char(" + colsize + ")";

            if (pgFieldType == "double precision")
                return "double";
            
            if (pgFieldType == "bytea")
                return "blob";
            
            if (pgFieldType == "timestamp without time zone")
                return "datetime";

            if (pgFieldType == "serial" || pgFieldType == "bigserial") 
                return "int(" + colsize + ")";

            // all the rest are correct
            return pgFieldType;
        }

        protected override List<ColumnDefinition> ExtractColumnsFromTable (string tableName)
        {
            var defs = new List<ColumnDefinition> ();
            tableName = tableName.ToLower ();
            IDataReader rdr = null;
            try
            {
                var qry = string.Format ("select column_name,data_type,column_default, character_maximum_length, is_nullable" +
                    " from information_schema.columns where table_name='{0}';",tableName);

                //rdr = Query (string.Format ("desc {0}", tableName), new Dictionary<string, object> ());
//                rdr = Query (qry);
                var cmd = Query(qry);
                rdr = cmd.ExecuteReader();
                while (rdr.Read ())
                {

                    var name = rdr ["column_name"];
                    //var pk = rdr["Key"];
                    var type = rdr ["data_type"];
                    //var extra = rdr["Extra"];
                    object defaultValue = rdr ["column_default"];

                    var colsize = rdr ["character_maximum_length"];
                    var pgType = ConvertPgTypeToColumnDef(type.ToString(), colsize.ToString());
                    ColumnTypeDef typeDef = ConvertTypeToColumnType (pgType);
                    //ColumnTypeDef typeDef = ConvertTypeToColumnType (type.ToString ());

                    typeDef.isNull = rdr ["is_nullable"].ToString () == "YES";
//                    typeDef.auto_increment = rdr ["serial"].ToString () == "YES";
                    typeDef.auto_increment = pgType == "serial";
                    typeDef.defaultValue = defaultValue is DBNull
                        ? null
                        : defaultValue.ToString ();
                    
                    defs.Add (new ColumnDefinition {
                        Name = name.ToString (),
                        Type = typeDef,
                    });
                }
                if (rdr != null)
                    rdr.Close ();
                cmd.Connection.Close();
                
            } catch (Exception e)
            {
                MainConsole.Instance.ErrorFormat ("[PgSQLDataLoader] ExtractColumnsFromTable: {0}", e);
            }// finally
            /*
            {
                try
                {
                    if (rdr != null)
                    {
                        rdr.Close ();
                        //rdr.Dispose ();
                    }
                } catch (Exception e)
                {
                    MainConsole.Instance.DebugFormat ("[PgSQLDataLoader] ExtractColumnsFromTable: {0}", e);
                }
            }
*/

            return defs;
        }

        protected override Dictionary<string, IndexDefinition> ExtractIndicesFromTable (string tableName)
        {
            var defs = new Dictionary<string, IndexDefinition> ();
            IDataReader rdr = null;
            var indexLookup = new Dictionary<string, Dictionary<uint, string>> ();
            var indexIsUnique = new Dictionary<string, bool> ();

            tableName = tableName.ToLower ();

            try
            {
                var idxqry = string.Format ("select indexdef from pg_indexes where tablename = '{0}'",tableName);
                var cmd  = Query (idxqry);
                rdr = cmd.ExecuteReader();
//                rdr = Query (idxqry);
                while (rdr.Read ())
                {
                    string idxdef = rdr ["indexdef"].ToString ().ToLower();
                    string idxstru = idxdef.Substring(0,idxdef.IndexOf("using"));
                    string idxcols = idxdef.Substring(idxdef.IndexOf("(")+1);
                    idxcols = idxcols.Remove(idxcols.Length-1);

                    string[] indexdef = idxdef.Split(' ');
                    string[] indexcols = idxcols.Split(',');
                    string name;
                    string index;
                    bool unique;

                    if (indexdef[1] == "unique")                // create unique index <indexname> on <tablename> using btree(<key1>, <key2>,...)
                    {
                        name = indexdef[3];
                        unique = true;
                    }
                    else
                    {
                        name = indexdef[2];                     // create index <indexname> on <tablename> using btree(<key1>, <key2>,...)
                        unique = false;
                    }
                          
                    // index type  _pkey: primary, _key: unique, _idx: general index
                    string[] idxname = name.Split('_');
                    string indexType = (idxname[idxname.Count() - 1]);
                    switch (indexType)
                    {
                    case "pkey":
                        index = "PRIMARY";
                        //unique = true;
                        break;
                    case "key":
//                        index = "UNIQUE";
                        index = idxname[1];     // idx_keyname_...
                        //unique = true;
                        break;
                    default:
//                        index = "INDEX";
                        index = idxname[1];
                        //unique = false;
                        break;
                    }

                    indexIsUnique[index] = unique;   // primary & unique  are 'unique', others are not
//                    indexIsUnique[index] = (idxname[idxname.Count() - 1] != "key");   // primary & unique  are 'unique', others are not

//                    if (!indexLookup.ContainsKey(index))       // this should be the case... check for multiple index versions though
//                    {
                        indexLookup[index] = new Dictionary<uint, string>();    // each record is an index
//                    }
                    uint sequence = 0;
                    foreach(string colname in indexcols)
                    {
                        indexLookup[index][sequence] = colname.Trim();
                        sequence += 1;
                    }

                 }
                if (rdr != null)
                    rdr.Close ();
                cmd.Connection.Close();

            } catch (Exception e)
            {
                MainConsole.Instance.ErrorFormat ("[PgSQLDataLoader] ExtractIndicesFromTable: {0}", e);
            } /* finally
            {
                try
                {
                    if (rdr != null)
                    {
                        rdr.Close ();
                    }
                } catch (Exception e)
                {
                    MainConsole.Instance.DebugFormat ("[PgSQLDataLoader] ExtractIndicesFromTable: {0}", e);
                }
            }*/

            foreach (KeyValuePair<string, Dictionary<uint, string>> index in indexLookup)
            {
                index.Value.OrderBy (x => x.Key);
                defs [index.Key] = new IndexDefinition {
                    Fields = index.Value.Values.ToArray<string> (),
                    Type = (indexIsUnique [index.Key]
                        ? (index.Key == "PRIMARY" ? IndexType.Primary : IndexType.Unique)
                        : IndexType.Index)
                };
            }

            return defs;
        }

        #endregion

        public override IGenericData Copy ()
        {
            return new PgSQLDataLoader ();
        }
    }
}