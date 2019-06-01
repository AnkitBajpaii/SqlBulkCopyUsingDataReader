using System.Data;

namespace SqlBulkCopyUsingDataReader
{
    public class ColumnMetadata
    {
        public ColumnMetadata()
        {
        }

        public ColumnMetadata(int ordinal, string name, SqlDbType dbType, bool isNullable)
        {
            this.Ordinal = ordinal;
            this.Name = name;
            this.DbType = dbType;
            this.IsNullable = isNullable;
        }


        public ColumnMetadata(int ordinal, string name, SqlDbType dbType, bool isNullable, long width)
        {
            this.Ordinal = ordinal;
            this.Name = name;
            this.DbType = dbType;
            this.IsNullable = isNullable;
            this.Width = width;
        }

        public int Ordinal { get; set; }

        public string Name { get; set; }

        public SqlDbType DbType { get; set; }

        public bool IsNullable { get; set; }
        public long Width { get; set; }
    }
}
