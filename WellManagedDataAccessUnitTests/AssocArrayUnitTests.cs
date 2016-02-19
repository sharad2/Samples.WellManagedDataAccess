
using HappyOracle.WellManagedDataAccess.Client;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;

namespace AssocArrayUnitTests
{
    [TestClass]
    public class AssocArrayTests
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
            __db.ExecuteNonQuery(@"
CREATE OR REPLACE PACKAGE MYPACK AS
  TYPE AssocArrayVarchar2_t is table of VARCHAR(20) index by BINARY_INTEGER;
  TYPE AssocArrayInteger_t is table of NUMBER(8) index by BINARY_INTEGER;
  TYPE AssocArrayLong_t is table of NUMBER(19) index by BINARY_INTEGER;
  PROCEDURE TestVarchar2(Param1 IN AssocArrayVarchar2_t,
                         Param2 IN OUT AssocArrayVarchar2_t,
                         Param3 OUT AssocArrayVarchar2_t);
  PROCEDURE TestInteger(Param1 IN AssocArrayInteger_t,
                        Param2 IN OUT AssocArrayInteger_t,
                        Param3 OUT AssocArrayInteger_t);
  PROCEDURE TestLong(Param1 IN AssocArrayLong_t,
                     Param2 IN OUT AssocArrayLong_t,
                     Param3 OUT AssocArrayLong_t);
END MYPACK;
            ");

            __db.ExecuteNonQuery(@"
create or replace package body MYPACK as
  PROCEDURE TestVarchar2(Param1 IN AssocArrayVarchar2_t,
                         Param2 IN OUT AssocArrayVarchar2_t,
                         Param3 OUT AssocArrayVarchar2_t) IS
    i integer;
  BEGIN
    -- Copy all elements of Param1 to Param 3
    FOR i IN Param1.FIRST .. Param1.LAST LOOP
      Param3(i) := Param1(i);
    END LOOP;
  
    -- Convert Param2 to upper case
    FOR i IN Param2.FIRST .. Param2.LAST LOOP
      Param2(i) := UPPER(Param2(i));
    END LOOP;
  
  END TestVarchar2;

  PROCEDURE TestInteger(Param1 IN AssocArrayInteger_t,
                        Param2 IN OUT AssocArrayInteger_t,
                        Param3 OUT AssocArrayInteger_t) IS
    i integer;
  BEGIN
    -- Copy all elements of Param1 to Param 3
    FOR i IN Param1.FIRST .. Param1.LAST LOOP
      Param3(i) := Param1(i);
    END LOOP;
  
    -- Double values in Param2
    FOR i IN Param2.FIRST .. Param2.LAST LOOP
      Param2(i) := Param2(i) * 2;
    END LOOP;
  END;

  PROCEDURE TestLong(Param1 IN AssocArrayLong_t,
                     Param2 IN OUT AssocArrayLong_t,
                     Param3 OUT AssocArrayLong_t) IS
  
    i integer;
  BEGIN
    -- Copy all elements of Param1 to Param 3
    FOR i IN Param1.FIRST .. Param1.LAST LOOP
      Param3(i) := Param1(i);
    END LOOP;
  
    -- Double values in Param2
    FOR i IN Param2.FIRST .. Param2.LAST LOOP
      Param2(i) := Param2(i) * 2;
    END LOOP;
  END;
END MYPACK;

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

        ///// <summary>
        ///// Executes before each test. Ensure that only predefined values exist in the departments table
        ///// Insert known values in an empty departments table
        ///// </summary>
        //[TestInitialize]
        //public void InitializeTest()
        //{
        //    var binder = SqlBinder.Create(_stdArrayDeptNo.Length)
        //        .Parameter("deptno", _stdArrayDeptNo)
        //        .Parameter("deptname", _stdArrayDeptName);

        //    var nRows = __db.ExecuteNonQuery("delete from departments");
        //    nRows = __db.ExecuteNonQuery(QUERY_INSERT, binder);
        //}

        /// <summary>
        /// Verify associative array binding for string arrays
        /// </summary>
        [TestMethod]
        public void AssocArray_StringParameters()
        {
            var values1 = new[] { "Input1", "Input2", "Input3" };

            // This will be bound to an input output parameter. After execution, all values will
            // convert to upper case
            var values2 = new[] { "Inout1", null, "Inout3" };

            // This will contain the out values of param3. They will be same as values of param1
            IList<string> values3 = null;
            IList<string> values2_out = null;

            var binder = WCommand.Create(@"
begin
    MyPack.TestVarchar2(:param1, :param2, :param3);
end;
            ")
                .ParameterAssociativeArray("param1", values1)
                .ParameterAssociativeArray("param2", values2)
                .OutParameterAssociativeArray("param2", values => values2_out = values.ToList(), 3, 255)
                .OutParameterAssociativeArray("param3", values => values3 = values.ToArray(), 3, 255);

            var nRows = binder.ExecuteNonQuery(__db);

            Assert.AreEqual(values1.Length, values3.Count);

            for (var i = 0; i < values1.Length; ++i)
            {
                Assert.AreEqual(values1[i], values3[i], "Param3 must be a copy of Param1");
            }

            for (var i = 0; i < values1.Length; ++i)
            {
                Assert.AreEqual(values2[i]?.ToUpper(), values2_out[i],
                    "Param 2 output values must be upper case of input values");
            }
        }

        /// <summary>
        /// Verify associative array binding for integer arrays
        /// </summary>
        [TestMethod]
        public void AssocArray_IntegerParameters()
        {
            var values1 = new[] { 2, 3, 4 };

            // This will be bound to an input output parameter. After execution, all values will
            // convert to upper case
            var values2 = new int?[] { 5, null, 7 };

            // This will contain the out values of param3. They will be same as values of param1
            IList<int?> values3 = null;
            IList<int?> values2_out = null;

            var binder = WCommand.Create(@"
begin
    MyPack.TestInteger(:param1, :param2, :param3);
end;
            ")
                .ParameterAssociativeArray("param1", values1)
                .ParameterAssociativeArray("param2", values2)
                .OutParameterAssociativeArray("param2", values => values2_out = values.ToList(), 3)
                .OutParameterAssociativeArray("param3", values => values3 = values.ToArray(), 3);

            var nRows = binder.ExecuteNonQuery(__db);

            Assert.AreEqual(values1.Length, values3.Count);

            for (var i = 0; i < values1.Length; ++i)
            {
                Assert.AreEqual(values1[i], values3[i], "Param3 must be a copy of Param1");
            }

            for (var i = 0; i < values1.Length; ++i)
            {
                Assert.AreEqual(values2[i] * 2, values2_out[i],
                    "Param 2 output values must be upper case of input values");
            }
        }

        /// <summary>
        /// Not allowed to bind null associative string arrays
        /// </summary>
        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void AssocArray_NullStringArray()
        {
            IList<string> valuesNull = null; // new int?[] { null};

            var binder = WCommand.Create("XXX")
                .ParameterAssociativeArray("param1", valuesNull);
        }

        /// <summary>
        /// Not allowed to bind empty associative string arrays
        /// </summary>
        [TestMethod]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void AssocArray_EmptySringArray()
        {
            IList<string> valuesNull = new string[0]; // new int?[] { null};

            var binder = WCommand.Create("xxxx")
                .ParameterAssociativeArray("param1", valuesNull);
        }

        /// <summary>
        /// Not allowed to bind null associative string arrays
        /// </summary>
        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void AssocArray_NullIntegerArray()
        {
            IList<int?> valuesNull = null; // new int?[] { null};

            var binder = WCommand.Create("XXX")
                .ParameterAssociativeArray("param1", valuesNull);
        }

        /// <summary>
        /// Not allowed to bind empty associative string arrays
        /// </summary>
        [TestMethod]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void AssocArray_EmptyIntegerArray()
        {
            IList<int?> valuesNull = new int?[0]; // new int?[] { null};

            var binder = WCommand.Create("XXX")
                .ParameterAssociativeArray("param1", valuesNull);
        }

        /// <summary>
        /// Verify associative array binding for integer arrays
        /// </summary>
        [TestMethod]
        public void AssocArray_LongParameters()
        {
            var values1 = new long[] { 2, 3, long.MaxValue };

            // This will be bound to an input output parameter. After execution, all values will
            // convert to upper case
            var values2 = new long?[] { 5, null, int.MaxValue };

            // This will contain the out values of param3. They will be same as values of param1
            IList<long?> values3 = null;
            IList<long?> values2_out = null;

            var binder = WCommand.Create(@"
begin
    MyPack.TestLong(:param1, :param2, :param3);
end;
            ")
                .ParameterAssociativeArray("param1", values1)
                .ParameterAssociativeArray("param2", values2)
                .OutParameterAssociativeArray("param2", values => values2_out = values.ToList(), 3)
                .OutParameterAssociativeArray("param3", values => values3 = values.ToArray(), 3);

            var nRows = binder.ExecuteNonQuery(__db);

            Assert.AreEqual(values1.Length, values3.Count);

            for (var i = 0; i < values1.Length; ++i)
            {
                Assert.AreEqual(values1[i], values3[i], "Param3 must be a copy of Param1");
            }

            for (var i = 0; i < values1.Length; ++i)
            {
                Assert.AreEqual(values2[i] * 2, values2_out[i],
                    "Param 2 output values must be double of input values");
            }
        }

    }
}
