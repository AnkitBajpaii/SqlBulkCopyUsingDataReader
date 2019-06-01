using Microsoft.SqlServer.Server;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace SqlBulkCopyUsingDataReader
{
    public abstract class EntityDataReader<T> : IDataReader, IEnumerable<SqlDataRecord>, IDisposable where T : class
    {
        IEnumerable<T> _dataList;
        IEnumerator<T> _dataEnumerator;
        private SqlMetaData[] _sqlMetadataList = null;
        private List<ColumnMetadata> _columnMetadataList = null;
        private Dictionary<int, ColumnMetadata> _ordinaltoColumnMetadataMappings;
        private Dictionary<string, ColumnMetadata> _nameToColumnMetadataMappings;
        public EntityDataReader(IEnumerable<T> data)
        {
            this._dataList = data;
            this._dataEnumerator = data.GetEnumerator();

            this.Initialize();
        }

        private void Initialize()
        {
            this._columnMetadataList = this.GetMetadata();
            this.FieldCount = this._columnMetadataList.Count;

            //Sort by ordinal
            this._columnMetadataList.OrderBy(item => item.Ordinal);

            this._sqlMetadataList = new SqlMetaData[this._columnMetadataList.Count];

            for (int i = 0; i < this._columnMetadataList.Count; i++)
            {
                var colMetadata = this._columnMetadataList[i];
                if (colMetadata.DbType == SqlDbType.NVarChar || colMetadata.DbType == SqlDbType.VarChar)
                {
                    this._sqlMetadataList[i] = new SqlMetaData(colMetadata.Name, colMetadata.DbType, colMetadata.Width);
                }

                else
                {
                    this._sqlMetadataList[i] = new SqlMetaData(colMetadata.Name, colMetadata.DbType);
                }
            }

            this._ordinaltoColumnMetadataMappings = this._columnMetadataList.ToDictionary(x => x.Ordinal, y => y);
            this._nameToColumnMetadataMappings = this._ordinaltoColumnMetadataMappings.ToDictionary(x => x.Value.Name, item => item.Value);
        }

        public abstract List<ColumnMetadata> GetMetadata();

        protected abstract object GetValue(int index, T data);

        public object this[int i] => this.GetValue(i);

        public object this[string name] => this.GetValue(this.GetOrdinal(name));

        public int Depth => 1;

        public bool IsClosed => this._dataEnumerator == null;

        public int RecordsAffected => -1;

        public int FieldCount { get; private set; } = 0;

        public void Close()
        {
            Dispose();
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(true);
        }

        public void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (this._dataEnumerator != null)
                {
                    this._dataEnumerator.Dispose();
                    this._dataEnumerator = null;
                    this._dataList = null;
                }
            }
        }

        ~EntityDataReader()
        {
            Dispose(false);
        }

        public bool GetBoolean(int i)
        {
            throw new NotImplementedException();
        }

        public byte GetByte(int i)
        {
            throw new NotImplementedException();
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public char GetChar(int i)
        {
            throw new NotImplementedException();
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public IDataReader GetData(int i)
        {
            throw new NotImplementedException();
        }

        public string GetDataTypeName(int i)
        {
            throw new NotImplementedException();
        }

        public DateTime GetDateTime(int i)
        {
            throw new NotImplementedException();
        }

        public decimal GetDecimal(int i)
        {
            throw new NotImplementedException();
        }

        public double GetDouble(int i)
        {
            throw new NotImplementedException();
        }

        public Type GetFieldType(int i)
        {
            throw new NotImplementedException();
        }

        public float GetFloat(int i)
        {
            throw new NotImplementedException();
        }

        public Guid GetGuid(int i)
        {
            throw new NotImplementedException();
        }

        public short GetInt16(int i)
        {
            throw new NotImplementedException();
        }

        public int GetInt32(int i)
        {
            throw new NotImplementedException();
        }

        public long GetInt64(int i)
        {
            throw new NotImplementedException();
        }

        public string GetName(int i)
        {
            return this._ordinaltoColumnMetadataMappings[i].Name;
        }

        public int GetOrdinal(string name)
        {
            return this._nameToColumnMetadataMappings[name].Ordinal;
        }

        public DataTable GetSchemaTable()
        {
            throw new NotImplementedException();
        }

        public string GetString(int i)
        {
            throw new NotImplementedException();
        }

        public object GetValue(int i)
        {
            return this.GetValue(i, this._dataEnumerator.Current);
        }

        public int GetValues(object[] values)
        {
            if (values == null)
            {
                throw new ArgumentNullException(nameof(values));
            }

            int copyLength = (values.Length < FieldCount) ? values.Length : FieldCount;

            for (int i = 0; i < copyLength; i++)
            {
                values[i] = GetValue(i);
            }

            return copyLength;
        }

        public bool IsDBNull(int i)
        {
            if (this._ordinaltoColumnMetadataMappings[i].IsNullable)
            {
                var val = this.GetValue(i);

                if (val == null) return true;
            }

            return false;
        }

        public bool NextResult()
        {
            return false;
        }

        public bool Read() => this._dataEnumerator.MoveNext();

        #region IEnumerable<SqlDataRecord> Members
        public IEnumerator<SqlDataRecord> GetEnumerator()
        {
            foreach (var item in this._dataList)
            {
                var record = new SqlDataRecord(this._sqlMetadataList);

                if (!this.Read()) break;

                var values = new object[this._sqlMetadataList.Length];
                this.GetValues(values);

                record.SetValues(values);

                yield return record;
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this._dataEnumerator;
        }
        #endregion
    }
}
