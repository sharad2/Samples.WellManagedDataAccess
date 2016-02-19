using Microsoft.VisualStudio.TestTools.UnitTesting;
using HappyOracle.WellManagedDataAccess.Helpers;
using HappyOracle.WellManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Linq;

namespace WellManagedDataAccessUnitTests
{
    [TestClass]
    public class NonQueryUnitTests
    {
        private const string CONNECT_STRING = "User Id=scott; Password=tiger; Data Source=localhost/dcmsprd1";

        private static WConnection __db = new WConnection(CONNECT_STRING);

        /// <summary>
        /// Standard departments inserted before the start of each test
        /// </summary>
        const string QUERY_INSERT = @"
			insert into departments (department_id, department_name)
    			values(:deptno, :deptname)";
        IList<int> _stdArrayDeptNo = new[] { 1, 2, 3 };
        string[] _stdArrayDeptName = new[] { "d1", "d2", "d3" };

        /// <summary>
        /// Executes before the start of the first test. Drop and recreate the DEPARTMENTS
        /// </summary>
        /// <param name="ctx"></param>
        [ClassInitialize]
        public static void Initialize(TestContext ctx)
        {
            try
            {
                __db.ExecuteNonQuery(@"drop table DEPARTMENTS");
            }
            catch (WOracleException ex) when (ex.OracleErrorNumber == OracleErrorNumber.TABLE_OR_VIEW_DOES_NOT_EXIST)
            {
                // No such table. Do nothing
            }
            __db.ExecuteNonQuery(@"create table DEPARTMENTS
(
  department_id   NUMBER(4) not null,
  department_name VARCHAR2(30),
  manager_id      NUMBER(6),
  location_id     NUMBER(4)
)");
            __db.ExecuteNonQuery(@"alter table DEPARTMENTS
  add constraint DEPT_ID_PK primary key (DEPARTMENT_ID)");

            __db.ExecuteNonQuery(@"
CREATE OR REPLACE PACKAGE TEST_NonQuery is
  TYPE refcursor is ref cursor;
  FUNCTION Ret1Cur return refCursor;
  PROCEDURE Get1CurOut(p_cursor1 out refCursor);
  PROCEDURE Double_This_Number(in_number in number, out_number OUT NUMBER);
end TEST_NonQuery;

");
            __db.ExecuteNonQuery(@"
create or replace package body TEST_NonQuery is
  FUNCTION Ret1Cur return refCursor is
    p_cursor refCursor;
  BEGIN
    open p_cursor for
      select * from departments;
    return(p_cursor);
  END Ret1Cur;
  PROCEDURE Get1CurOut(p_cursor1 out refCursor) is
  BEGIN
    OPEN p_cursor1 for
      select * from departments;
  END Get1CurOut;
  PROCEDURE Double_This_Number(in_number in number, out_number OUT NUMBER) IS
  BEGIN
    out_number := in_number * 2;
  END Double_This_Number;
end TEST_NonQuery;
");
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
        /// Executes before each test. Ensure that only predefined values exist in the departments table
        /// Insert known values in an empty departments table
        /// </summary>
        [TestInitialize]
        public void InitializeTest()
        {
            var binder = WCommand.Create(QUERY_INSERT, _stdArrayDeptNo.Count)
                .Parameter("deptno", _stdArrayDeptNo)
                .Parameter("deptname", _stdArrayDeptName);

            var nRows = __db.ExecuteNonQuery("delete from departments");
            nRows = binder.ExecuteNonQuery(__db);
        }

        [TestMethod]
        public void Nonquery_InvalidQueryNoParameters()
        {
            try
            {
                __db.ExecuteNonQuery(@"
<query>
Hello
</query>
");
            }
            catch (WOracleException ex) when (ex.OracleErrorNumber == OracleErrorNumber.INVALID_SQL_STATEMENT)
            {
                // Query reached oracle so we are happy
                return;
            }

            Assert.Fail("We were expecting ORA-900 Invalid SQL");
        }

        /// <summary>
        /// Query requires parameter but it was not provided
        /// </summary>
        [TestMethod]
        public void Nonquery_QueryWithParametersNoBinder()
        {
            try
            {
                __db.ExecuteNonQuery(@"
select 1 from dual where :badparam is null
");
            }
            catch (WOracleException ex) when (ex.OracleErrorNumber == OracleErrorNumber.NOT_ALL_VARIABLES_BOUND)
            {
                return;
            }
            Assert.Fail("Expected ORA-1008 not all variables bound");
        }

        /// <summary>
        /// Ref cursor returns select * from departments
        /// </summary>
        [TestMethod]
        public void Nonquery_OutRefCursor()
        {

            IList<int?> list = null;

            var cmd = WCommand.Create(@"
<query>
BEGIN
    :out_cursor := TEST_NonQuery.Ret1Cur;
END;
</query>
")
                .OutRefCursorParameter("out_cursor", rows => list = rows.Select(p => p.GetInteger(0)).ToList());
            cmd.ExecuteNonQuery(__db);
            Assert.IsNotNull(list);
            Assert.AreEqual(_stdArrayDeptNo.Count, list.Count);
            Assert.IsTrue(_stdArrayDeptNo.Cast<int?>().SequenceEqual(list));

        }

        [TestMethod]
        public void Nonquery_NullOutRefCursor()
        {

            IList<int?> list = null;

            var cmd = WCommand.Create(@"
BEGIN
    :out_cursor := NULL;
END;
")
                .OutRefCursorParameter("out_cursor", rows => list = rows.Select(p => p.GetInteger(0)).ToList());
            cmd.ExecuteNonQuery(__db);
            Assert.IsNull(list);

        }

        [TestMethod]
        public void Nonquery_IntInOutParameter()
        {
            const string QUERY = @"
begin
  -- Call the procedure
  test_nonquery.double_this_number(in_number => :in_number,
                                   out_number => :out_number);
end;
";
            int input = 32;
            int? output = null;
            var cmd = WCommand.Create(QUERY)
                .Parameter("in_number", input)
                .OutParameter("out_number", val => output = val);
            cmd.ExecuteNonQuery(__db);
            Assert.AreEqual(input * 2, output);
        }

        [TestMethod]
        public void Nonquery_LongInOutParameter()
        {
            const string QUERY = @"
begin
  -- Call the procedure
  test_nonquery.double_this_number(in_number => :in_number,
                                   out_number => :out_number);
end;
";
            long input = 32;
            long? output = null;
            var cmd = WCommand.Create(QUERY)
                .Parameter("in_number", input)
                .OutParameter("out_number", val => output = val);
            cmd.ExecuteNonQuery(__db);
            Assert.AreEqual(input * 2, output);
        }

        [TestMethod]
        public void Nonquery_StringInOutParameter()
        {
            const string QUERY = @"
begin
    :str3 := :str1 || :str2;
end;
";
            string str1 = "Sharad";
            string str2 = "Singhal";
            string str3 = null;
            var cmd = WCommand.Create(QUERY)
                .Parameter("str1", str1)
                .Parameter("str2", str2)
                .OutParameter("str3", val => str3 = val);
            cmd.ExecuteNonQuery(__db);
            Assert.AreEqual(str1 + str2, str3);
        }

        [TestMethod]
        public void Nonquery_DateTimeOffsetInOutParameter()
        {
            const string QUERY = @"
begin
    select (:client_timestamp + INTERVAL '1' DAY)
    INTO :client_timestamp
    from dual;
end;
";
            DateTimeOffset clientTimeOriginal = DateTimeOffset.Now;
            DateTimeOffset? clientTime = clientTimeOriginal;
            var cmd = WCommand.Create(QUERY)
                .Parameter("client_timestamp", clientTime)
                .OutParameter("client_timestamp", (DateTimeOffset? val) => clientTime = val);
            cmd.ExecuteNonQuery(__db);
            Assert.AreEqual(clientTimeOriginal.AddDays(1), clientTime);
        }

        [TestMethod]
        public void Nonquery_ReturningClause()
        {
            const string QUERY_DELETE = @"
            delete from departments
             where department_id = :deptno
         RETURNING department_id, department_name
              INTO :deptno_out, :deptname_out";

            // Prepare to receive RETURNING values
            string deptName = null;
            int? deptNumber = null;

            var cmd = WCommand.Create(QUERY_DELETE)
                .Parameter("deptno", _stdArrayDeptNo[0])
                .OutParameter("deptno_out",
                    val => deptNumber = val)
                .OutParameter("deptname_out",
                    val => deptName = val);

            var nRows = cmd.ExecuteNonQuery(__db);
            Assert.AreEqual(1, nRows);
            Assert.AreEqual(_stdArrayDeptNo[0], deptNumber);
            Assert.AreEqual(_stdArrayDeptName[0], deptName);
        }

    }
}
