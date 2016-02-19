using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using HappyOracle.WellManagedDataAccess.Client;
using System.Collections.Generic;
using System.Data;

//using System.Configuration;
//using HappyOracle.WellManagedDataAccess.Helpers;
//using HappyOracle.ManagedDataAccess;

namespace WellManagedDataAccessUnitTests
{
    [TestClass]
    public class DiagnosticUnitTests
    {

        private const string CONNECT_STRING = "User Id=scott; Password=tiger; Data Source=localhost/dcmsprd1";

        private static WConnection __db = new WConnection(CONNECT_STRING);

        /// <summary>
        /// Executes before the start of the first test. Drop and recreate the DEPARTMENTS
        /// </summary>
        /// <param name="ctx"></param>
        [ClassInitialize]
        public static void Initialize(TestContext ctx)
        {

        }

        /// <summary>
        /// Dispose the database connection
        /// </summary>
        [ClassCleanup]
        public static void Cleanup()
        {
            __db.Dispose();
            __db = null;
        }

        /// <summary>
        /// Trying to retrieve string as integer
        /// </summary>
        [TestMethod]
        [TestCategory("sharad category")]
        public void Diag_WrongColumnType()
        {
            const string QUERY = @"
select 1 as col1
from dual
";
            var binder = WCommand.Create(QUERY, row => row.GetString(0));

            try {
                binder.ExecuteSingle(__db);
            }
            catch (InvalidCastException ex)
            {
                Assert.Inconclusive(ex.Message);
            }
            Assert.Fail("Expected exception");
        }

        /// <summary>
        /// Trying to retrieve a non existent column
        /// </summary>
        [TestMethod]
        [TestCategory("sharad category")]
        public void Diag_BadColumn()
        {
            const string QUERY = @"
select 1 as col1
from dual
";
            var binder = WCommand.Create(QUERY, row => row.GetString("col2"));

            try
            {
                binder.ExecuteSingle(__db);
            }
            catch (KeyNotFoundException ex)
            {
                Assert.Inconclusive(ex.Message);
            }
            Assert.Fail("Expected exception");
        }

        /// <summary>
        /// Trying to retrieve a non existent column
        /// </summary>
        [TestMethod]
        public void Diag_DuplicateColumnName()
        {
            const string QUERY = @"
select 1 as col1, 2 as col1
from dual
";
            var binder = WCommand.Create(QUERY, row => row.GetString("col1"));

            try
            {
                binder.ExecuteSingle(__db);
            }
            catch (DuplicateNameException ex)
            {
                Assert.Inconclusive(ex.Message);
            }
            Assert.Fail("Expected exception");
        }

        /// <summary>
        /// No value has been provided for parameter sharad
        /// </summary>
        [TestMethod]
        public void Diag_BadParameter()
        {
            const string QUERY = @"
select 1 as col1
from dual
where something = :sharad
";
            var binder = WCommand.Create(QUERY, row => row.GetString("col1"));

            try
            {
                binder.ExecuteSingle(__db);
            }
            catch (KeyNotFoundException ex)
            {
                Assert.Inconclusive(ex.Message);
            }
            Assert.Fail("Expected exception");
        }

        [TestMethod]
        public void Diag_AddSameParameterDifferentDataTypes()
        {
            const string QUERY = @"
begin
  Not used
end;
";
            try {
                DateTimeOffset? clientTime = DateTimeOffset.Now;
                var cmd = WCommand.Create(QUERY)
                    .Parameter("client_timestamp", clientTime)  // Adding as date time offset
                    .Parameter("client_timestamp", 5);          // Adding as int
            }
            catch (ArgumentException ex)
            {
                Assert.Inconclusive(ex.Message);
            }
            Assert.Fail("Expected exception");
        }
    }
}
