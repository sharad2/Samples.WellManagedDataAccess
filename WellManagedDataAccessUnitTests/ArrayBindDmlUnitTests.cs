using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using HappyOracle.WellManagedDataAccess.Client;
using HappyOracle.WellManagedDataAccess.Helpers;
using System.Linq;
using System.Collections.Generic;

namespace ArrayBindDmlUnitTests
{
    [TestClass]
    public class ArrayBindUnitTests
    {
        private const string CONNECT_STRING = "User Id=scott; Password=tiger; Data Source=localhost/dcmsprd1";

        private static WConnection __db = new WConnection(CONNECT_STRING);

        /// <summary>
        /// Standard departments inserted before the start of each test
        /// </summary>
        const string QUERY_INSERT = @"
			insert into departments (department_id, department_name)
    			values(:deptno, :deptname)";
        int[] _stdArrayDeptNo = new[] { 1, 2, 3 };
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
            var binder = WCommand.Create(QUERY_INSERT, _stdArrayDeptNo.Length)
                .Parameter("deptno", _stdArrayDeptNo)
                .Parameter("deptname", _stdArrayDeptName);

            var nRows = __db.ExecuteNonQuery("delete from departments");
            nRows = binder.ExecuteNonQuery(__db);
        }

        /// <summary>
        /// Insert four unique rows and assert that ExecuteNonQuery returns the number of rows inserted
        /// </summary>
        [TestMethod]
        public void ArrayBind_InsertNewDepartments()
        {
            var myArrayDeptNo = new int[] { 4, 5, 6 };
            var myArrayDeptName = new string[] { "d4", "d5", "d6" };

            var binder = WCommand.Create(QUERY_INSERT, myArrayDeptNo.Length)
                .Parameter("deptno", myArrayDeptNo)
                .Parameter("deptname", myArrayDeptName);

            var nRows = binder.ExecuteNonQuery(__db);

            Assert.AreEqual(binder.ArrayBindCount, nRows);
        }

        /// <summary>
        /// Array bind size is 4, but we provide only 3 values for department name.
        /// The parameter function should raise ArgumentException
        /// </summary>
        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void ArrayBind_InsertNewDepartments_InsufficientBindValues()
        {
            var myArrayDeptNo = new int[] { 4, 5, 6, 7 };
            var myArrayDeptName = new string[] { "d4", "d5", "d6" };

            var binder = WCommand.Create("XXX", myArrayDeptNo.Length)
                .Parameter("deptno", myArrayDeptNo)
                .Parameter("deptname", myArrayDeptName);
        }

        /// <summary>
        /// In each array we provide more values than the array bind count. The extra values are ignored.
        /// </summary>
        [TestMethod]
        public void ArrayBind_InsertNewDepartments_TooManyBindValues()
        {
            var myArrayDeptNo = new int[] { 4, 5, 6, 7 };
            var myArrayDeptName = new string[] { "d4", "d5", "d6", "d7" };

            int bindCount = myArrayDeptNo.Length - 1;

            var binder = WCommand.Create(QUERY_INSERT, bindCount)
                .Parameter("deptno", myArrayDeptNo)
                .Parameter("deptname", myArrayDeptName);

            Assert.AreEqual(bindCount, binder.ArrayBindCount);

            var nRows = binder.ExecuteNonQuery(__db);

            Assert.AreEqual(bindCount, nRows);
        }

        /// <summary>
        /// Department Number 1 already exists. Therefore that row should get inserted
        /// </summary>
        [TestMethod]
        public void ArrayBind_InsertSomeBadRows()
        {
            var myArrayDeptNo = new int[] { 4, 5, 1, 1 };
            var myArrayDeptName = new string[] { "d4", "d5", "d6", "d7" };

            const int BIND_COUNT = 4;

            var binder = WCommand.Create(QUERY_INSERT, BIND_COUNT)
                .Parameter("deptno", myArrayDeptNo)
                .Parameter("deptname", myArrayDeptName);
            Assert.AreEqual(BIND_COUNT, binder.ArrayBindCount);

            try
            {
                binder.ExecuteNonQuery(__db);
                Assert.Fail($"{nameof(WOracleArrayBindException)} should have been raised");
            }
            catch (WOracleArrayBindException ex)
            {
                // All rows where dept number is 1 should cause a unique key error. We match error counts here
                Assert.AreEqual(myArrayDeptNo.Where(p => p == 1).Count(), ex.RowErrors.Count);

                foreach (var item in ex.RowErrors)
                {
                    // Assert that each failing row corresponds to department number 1
                    Assert.AreEqual(1, myArrayDeptNo[item.Key]);
                }
            }

            //Assert.Fail("ExecuteNonQuery should have thrown an exception");
        }

        /// <summary>
        /// We use a transaction to commit the good rows
        /// </summary>
        [TestMethod]
        public void ArrayBind_CommitGoodRowsIgnoreBadRows()
        {
            var myArrayDeptNo = new int[] { 4, 5, 1, 1 };
            var myArrayDeptName = new string[] { "d4", "d5", "d6", "d7" };

            //const int BIND_COUNT = 4;

            var binder = WCommand.Create(QUERY_INSERT, myArrayDeptNo.Length)
                .Parameter("deptno", myArrayDeptNo)
                .Parameter("deptname", myArrayDeptName);

            Assert.AreEqual(myArrayDeptNo.Length, binder.ArrayBindCount);

            using (var trans = __db.BeginTransaction())
            {
                try
                {
                    var nRows = binder.ExecuteNonQuery(__db);
                    Assert.Fail("Above query should have raised exception");
                }
                catch (WOracleArrayBindException ex)
                {
                    foreach (var item in ex.RowErrors)
                    {
                        // Assert that each failing row corresponds to department number 1
                        Assert.AreEqual(1, myArrayDeptNo[item.Key]);
                    }
                }

                // Commit the successful rows
                trans.Commit();
            }

            const string QUERY = "select department_id from departments where department_id = :deptno";
            var binder2 = WCommand.Create(QUERY, row => row.GetInteger(0));

            // Verify that each row which had dept num other than 1 was actually inserted
            foreach (var id in myArrayDeptNo.Where(p => p != 1))
            {
                binder2.Parameter("deptno", id);
                var x = binder2.ExecuteSingle(__db);
                Assert.AreEqual(id, x);
            }
        }

        /// <summary>
        /// Ensure that the RETURNING clause returns proper values as rows are being deleted.
        /// </summary>
        [TestMethod]
        public void ArrayBind_ReturningValuesBeingDeleted()
        {
            const string QUERY_DELETE = @"
        delete from departments
         where department_id = :deptno
     RETURNING department_id, department_name
          INTO :deptno_out, :deptname_out";

            // Prepare to receive RETURNING values
            IList<string> deptNames = null;
            IList<int> deptNumbers = null;

            var binder = WCommand.Create(QUERY_DELETE, _stdArrayDeptNo.Length)
                .Parameter("deptno", _stdArrayDeptNo)
                .OutParameter("deptno_out",
                    values => deptNumbers = values.ToList())
                .OutParameter("deptname_out",
                    values => deptNames = values.ToList());

            Assert.AreEqual(_stdArrayDeptNo.Length, binder.ArrayBindCount);

            var nRows = binder.ExecuteNonQuery(__db);

            // Verify that the returned values are same as the standard values in the departments table
            for (var i = 0; i < nRows; ++i)
            {
                Assert.AreEqual(_stdArrayDeptNo[i], deptNumbers[i]);
                Assert.AreEqual(_stdArrayDeptName[i], deptNames[i]);
            }
        }

    }
}
