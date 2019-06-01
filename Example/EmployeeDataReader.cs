using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlBulkCopyUsingDataReader.Example
{
    public class Employee
    {
        public string Name { get; set; }
        public int Age { get; set; }

        public double Salary { get; set; }
    }

    public class EmployeeDataReader : EntityDataReader<Employee>
    {
        public EmployeeDataReader(IEnumerable<Employee> data) : base(data)
        {
        }

        public override List<ColumnMetadata> GetMetadata()
        {
            return new List<ColumnMetadata>
            {
                new ColumnMetadata(0, "Name", System.Data.SqlDbType.VarChar, false),
                new ColumnMetadata(1, "Age", System.Data.SqlDbType.Int, false),
                new ColumnMetadata(2, "Salary", System.Data.SqlDbType.BigInt, true)
            };
        }

        protected override object GetValue(int index, Employee data)
        {
            switch (index)
            {
                case 0:
                    return data.Name;
                case 1:
                    return data.Age;
                case 2:
                    return data.Salary;
                default:
                    break;
            }

            return null;
        }
    }

    public class Client
    {
        public void BulkInsertEmployees(List<Employee> data)
        {
            using (SqlConnection conn = new SqlConnection("specify connection string"))
            {
                using (var reader = new EmployeeDataReader(data))
                {
                    using (var sqlBulkCopy = new SqlBulkCopy(conn, SqlBulkCopyOptions.Default, null))
                    {
                        sqlBulkCopy.BulkCopyTimeout = 0;
                        sqlBulkCopy.EnableStreaming = true;
                        sqlBulkCopy.BatchSize = 100000;
                        sqlBulkCopy.DestinationTableName = "EmployeeTable";

                        sqlBulkCopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(0, "Name"));
                        sqlBulkCopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(1, "Age"));
                        sqlBulkCopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(2, "Salary"));

                        sqlBulkCopy.WriteToServer(reader);
                    }
                }
            }
        }
    }
}
