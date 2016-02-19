using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Text.RegularExpressions;
using System.Xml;
using HappyOracle.WellManagedDataAccess.Helpers;
using System.Collections.Generic;
using System.Linq;

namespace WellManagedDataAccessUnitTests
{
    [TestClass]
    public class QueryPruningUnitTests
    {
        private readonly IDictionary<string, int> _dictRepeats = new Dictionary<string, int>
        {
            ["paramStringList"] = 3
        };

        private IDictionary<string, object> _dictValues;

        [TestInitialize]
        public void InitializeTest()
        {
            _dictValues = new Dictionary<string, object>
            {
                ["paramStringEmpty"] = "",
                ["paramStringNonNull"] = "sharad",
                ["paramStringEmpty2"] = "",
                ["paramStringNonNull2"] = "sharad2",
                ["paramStringList"] = new[] { "p1", "p2" },
                ["paramStringListEmpty"] = null
            };
        }

        /// <summary>
        /// The if clause containing a null parameter is gobbled up
        /// </summary>
        [TestMethod]
        public void QueryPrune_NullParameter()
        {
            //var binder = SqlBinder.Create()
            //    .Parameter("param1", "");

            const string QUERY = @"
            <query>
            Hi there
            <if> Now you see me :paramStringEmpty </if>
  
              </query>
";

            var queryOut = XmlToSql.BuildQuery(QUERY, _dictValues, _dictRepeats);

            string normalized1 = Regex.Replace(queryOut, @"\s+", " ").Trim();
            Assert.AreEqual("Hi there", normalized1, "query text");

            var usedParams = XmlToSql.GetParametersUsed(queryOut);

            Assert.AreEqual(0, usedParams.Length, "Number of parameters in query");
        }

        /// <summary>
        /// The if clause containing a non null parameter sticks around
        /// </summary>
        [TestMethod]
        public void QueryPrune_NonNullParameter()
        {

            const string QUERY = @"
            <query>
            Hi there
            <if> Now you see me :paramStringNonNull </if>
  
              </query>
";


            var queryOut = XmlToSql.BuildQuery(QUERY, _dictValues, _dictRepeats);

            //var cmd = __db.CreateCommand(QUERY, binder);

            string normalized1 = Regex.Replace(queryOut, @"\s+", " ").Trim();
            Assert.AreEqual("Hi there Now you see me :paramStringNonNull", normalized1);

            var usedParams = XmlToSql.GetParametersUsed(queryOut);
            Assert.AreEqual(1, usedParams.Length);
            Assert.AreEqual("paramStringNonNull", usedParams[0]);
            //Assert.Inconclusive(cmd.CommandText);
        }

        /// <summary>
        /// Since param1 is null, else clause will be used
        /// </summary>
        [TestMethod]
        public void QueryPrune_ElseClauseIsUsed()
        {
            const string QUERY = @"
<query>
            Hi there
            <if> Now you see me :paramStringEmpty </if>
<else>No paramStringEmpty given</else>
</query>
";
            var queryOut = XmlToSql.BuildQuery(QUERY, _dictValues, _dictRepeats);
            //var cmd = __db.CreateCommand(query, binder);

            string normalized1 = Regex.Replace(queryOut, @"\s+", " ").Trim();
            Assert.AreEqual("Hi there No paramStringEmpty given", normalized1);
            Assert.AreEqual(0, XmlToSql.GetParametersUsed(queryOut).Length);
        }

        /// <summary>
        /// Since param1 is null, else clause will be used
        /// </summary>
        [TestMethod]
        public void QueryPrune_ElseClauseIsNotUsed()
        {

            var query = @"
<query>
            Hi there
            <if> Now you see me :paramStringNonNull </if>
<else>No paramStringNonNull given</else>
</query>
";

            var queryOut = XmlToSql.BuildQuery(query, _dictValues, _dictRepeats);
            var usedParams = XmlToSql.GetParametersUsed(queryOut);

            string normalized1 = Regex.Replace(queryOut, @"\s+", " ").Trim();
            Assert.AreEqual("Hi there Now you see me :paramStringNonNull", normalized1);


            Assert.AreEqual(1, usedParams.Length);
            Assert.AreEqual("paramStringNonNull", usedParams[0]);
        }

        /// <summary>
        /// query XML is malformed because of </else
        /// </summary>
        [TestMethod]
        [ExpectedException(typeof(XmlException))]
        public void QueryPrune_MalformedXml()
        {
            var query = @"
<query>
            Hi there
            <if> Now you see me :param1 </if>
<else>No param1 given</else
</query>
";
            var queryOut = XmlToSql.BuildQuery(query, _dictValues, _dictRepeats);
            var usedParams = XmlToSql.GetParametersUsed(queryOut);
        }

        /// <summary>
        /// Missing parameter param2 within if
        /// </summary>
        [TestMethod]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void QueryPrune_MissingParameterWithinIf()
        {
            var query = @"
<query>
            Hi there
            <if> Now you see me :param1 and :param2</if>
<else>No param1 given</else>
</query>
";
            var queryOut = XmlToSql.BuildQuery(query, _dictValues, _dictRepeats);
            var usedParams = XmlToSql.GetParametersUsed(queryOut);
        }

        /// <summary>
        /// Multiple parameters param1 and param2 within if. All of them have null values so
        /// the text within <if></if> does not survive
        /// </summary>
        [TestMethod]
        public void QueryPrune_MultipleParametersWithinIfAllNull()
        {
            var query = @"
<query>
            Hi there
            <if> Now you see me :paramStringEmpty and :paramStringEmpty2</if>
<else>paramStringEmpty and paramStringEmpty2 are null</else>
</query>
";
            var queryOut = XmlToSql.BuildQuery(query, _dictValues, _dictRepeats);
            var usedParams = XmlToSql.GetParametersUsed(queryOut);
            string normalized = Regex.Replace(queryOut, @"\s+", " ").Trim();

            Assert.AreEqual("Hi there paramStringEmpty and paramStringEmpty2 are null", normalized);
            Assert.AreEqual(0, usedParams.Length);
            //Assert.Inconclusive(cmd.CommandText);
        }

        /// <summary>
        /// Multiple parameters param1 and param2 within if. One of them has null value, other is non null.
        /// The text within <if></if> survives only if all parameters are non null. Here it does not survive
        /// </summary>
        [TestMethod]
        public void QueryPrune_MultipleParametersWithinIfSomeNonNull()
        {
            var query = @"
<query>
            Hi there
            <if> Now you see me :paramStringEmpty and :paramStringNonNull</if>
<else>paramStringEmpty and paramStringNonNull are null</else>
</query>
";
            var queryOut = XmlToSql.BuildQuery(query, _dictValues, _dictRepeats);
            var usedParams = XmlToSql.GetParametersUsed(queryOut);
            string normalized = Regex.Replace(queryOut, @"\s+", " ").Trim();

            Assert.AreEqual("Hi there paramStringEmpty and paramStringNonNull are null", normalized);
            Assert.AreEqual(0, usedParams.Length);
            //Assert.Inconclusive(cmd.CommandText);
        }

        [TestMethod]
        public void QueryPrune_XmlArrayParameterNonNullList()
        {
            var query = @"
<query>
        WHERE 1=1
    <if>
        AND EDIPS.EDI_ID IN (
            <a sep=', '>:paramStringList</a>
        )
    </if>
</query>
";
            var queryOut = XmlToSql.BuildQuery(query, _dictValues, _dictRepeats);
            var usedParams = XmlToSql.GetParametersUsed(queryOut);
            string normalized = Regex.Replace(queryOut, @"\s+", " ").Trim();

            Assert.AreEqual(_dictRepeats["paramStringList"], usedParams.Length);
            Assert.AreEqual("WHERE 1=1 AND EDIPS.EDI_ID IN ( :paramStringList0, :paramStringList1, :paramStringList2 )",
                normalized);
        }

        /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void QueryPrune_XmlArrayParameterNullList()
        {
            var query = @"
<query>
        WHERE 1=1
<if>
AND EDIPS.EDI_ID IN (
<a sep=', '>:paramStringListEmpty</a>
)
</if>
</query>
";
            //     
            var queryOut = XmlToSql.BuildQuery(query, _dictValues, _dictRepeats);
            var usedParams = XmlToSql.GetParametersUsed(queryOut);
            string normalized = Regex.Replace(queryOut, @"\s+", " ").Trim();
            Assert.AreEqual(0, usedParams.Length);
            Assert.AreEqual("WHERE 1=1", normalized);
        }



        /// <summary>
        /// Multiple parameters param1 and param2 within if. One of them has null value, other is non null.
        /// The text within <if></if> survives only if all parameters are non null. Here it does not survive
        /// </summary>
        [TestMethod]
        public void QueryPrune_MultipleParametersWithinIfAllNonNull()
        {
            var query = @"
<query>
            Hi there
            <if> Now you see me :paramStringNonNull and :paramStringNonNull2</if>
<else>param1 and param2 are null</else>
</query>
";
            var queryOut = XmlToSql.BuildQuery(query, _dictValues, _dictRepeats);
            var usedParams = XmlToSql.GetParametersUsed(queryOut);
            string normalized = Regex.Replace(queryOut, @"\s+", " ").Trim();

            Assert.AreEqual("Hi there Now you see me :paramStringNonNull and :paramStringNonNull2", normalized);
            Assert.AreEqual(2, usedParams.Length);
            Assert.IsTrue(usedParams.Contains("paramStringNonNull"));
            Assert.IsTrue(usedParams.Contains("paramStringNonNull2"));
        }

        [TestMethod]
        public void QueryPrune_MultipleElsif()
        {
            var query = @"
<query>
        You chose
    <if c='$choice = 1'>
        choice 1
    </if>
    <elsif c='$choice = 2'>
        choice 2
    </elsif>
    <elsif c='$choice = 3'>
        choice 3
    </elsif>
    <else>
        bad choice
    </else>
</query>
";
            _dictValues["choice"] = 3;
            var queryOut = XmlToSql.BuildQuery(query, _dictValues, _dictRepeats);
            string normalized = Regex.Replace(queryOut, @"\s+", " ").Trim();
            Assert.AreEqual("You chose choice 3", normalized);

            _dictValues["choice"] = 1;
            queryOut = XmlToSql.BuildQuery(query, _dictValues, _dictRepeats);
            normalized = Regex.Replace(queryOut, @"\s+", " ").Trim();
            Assert.AreEqual("You chose choice 1", normalized);

            _dictValues["choice"] = 9;
            queryOut = XmlToSql.BuildQuery(query, _dictValues, _dictRepeats);
            normalized = Regex.Replace(queryOut, @"\s+", " ").Trim();
            Assert.AreEqual("You chose bad choice", normalized);
            //Assert.Inconclusive(cmd.CommandText);
        }

        [TestMethod]
        public void QueryPrune_NestedIfAndElse()
        {

            var query = @"
<query>
        begin if
    <if c='$choice'>
        :choice given
        <if>:subchoice given</if>
        <else>No subchoice</else>
        is good
    </if>
    <else>
        other choice
        <if>your :subchoice</if>
        <else>No subchoice</else>
        is bad
    </else>
    end if
</query>
";
            // Choice and shubchoice non null
            _dictValues["choice"] = 1;
            _dictValues["subchoice"] = 1;
            var queryOut = XmlToSql.BuildQuery(query, _dictValues, _dictRepeats);
            var usedParams = XmlToSql.GetParametersUsed(queryOut);
            string normalized = Regex.Replace(queryOut, @"\s+", " ").Trim();

            Assert.AreEqual(2, usedParams.Length);
            Assert.IsTrue(usedParams.Contains("subchoice"));
            Assert.IsTrue(usedParams.Contains("choice"));
            Assert.AreEqual("begin if :choice given :subchoice given is good end if", normalized);

            // Choice and shubchoice null
            _dictValues["choice"] = null;
            _dictValues["subchoice"] = null;
            queryOut = XmlToSql.BuildQuery(query, _dictValues, _dictRepeats);
            usedParams = XmlToSql.GetParametersUsed(queryOut);
            normalized = Regex.Replace(queryOut, @"\s+", " ").Trim();
            Assert.AreEqual(0, usedParams.Length);
            Assert.AreEqual("begin if other choice No subchoice is bad end if", normalized);

            // Choice null, shubchoice non null
            _dictValues["choice"] = null;
            _dictValues["subchoice"] = "hello";
            queryOut = XmlToSql.BuildQuery(query, _dictValues, _dictRepeats);
            usedParams = XmlToSql.GetParametersUsed(queryOut);
            normalized = Regex.Replace(queryOut, @"\s+", " ").Trim();
            Assert.AreEqual(1, usedParams.Length);
            Assert.IsTrue(usedParams.Contains("subchoice"));
            Assert.AreEqual("begin if other choice your :subchoice is bad end if", normalized);

            // Choice non null, shubchoice null
            _dictValues["choice"] = "hello";
            _dictValues["subchoice"] = null;
            queryOut = XmlToSql.BuildQuery(query, _dictValues, _dictRepeats);
            usedParams = XmlToSql.GetParametersUsed(queryOut);
            normalized = Regex.Replace(queryOut, @"\s+", " ").Trim();
            Assert.AreEqual(1, usedParams.Length);
            Assert.IsTrue(usedParams.Contains("choice"));
            Assert.AreEqual("begin if :choice given No subchoice is good end if", normalized);
        }

        /// <summary>
        /// Here the outer if does not specify an explicit condition
        /// </summary>
        [TestMethod]
        public void QueryPrune_NestedIfAndElse2()
        {

            const string QUERY = @"
	<query>
		SELECT *
		  FROM employees
		 WHERE
		<if c='$salary or $hire_date'>
			1 = 1
			<if>AND salary &gt;= :salary</if>
			<if>AND hire_date &gt;= :hire_date</if>
        </if>
		<else>
			rownum &lt; 20
		</else>
	</query>
";

            // Both null

            _dictValues["salary"] = null;
            _dictValues["hire_date"] = null;

            var queryOut = XmlToSql.BuildQuery(QUERY, _dictValues, _dictRepeats);
            var usedParams = XmlToSql.GetParametersUsed(queryOut);
            var normalized = Regex.Replace(queryOut, @"\s+", " ").Trim();
            Assert.AreEqual(0, usedParams.Length);
            Assert.AreEqual("SELECT * FROM employees WHERE rownum < 20", normalized,
                "Salary and hire date both null");

            // Salary null; hire_date non null
            _dictValues["salary"] = null;
            _dictValues["hire_date"] = 222;
            queryOut = XmlToSql.BuildQuery(QUERY, _dictValues, _dictRepeats);
            usedParams = XmlToSql.GetParametersUsed(queryOut);
            normalized = Regex.Replace(queryOut, @"\s+", " ").Trim();
            Assert.AreEqual(1, usedParams.Length);
            Assert.IsTrue(usedParams.Contains("hire_date"));
            Assert.AreEqual("SELECT * FROM employees WHERE 1 = 1 AND hire_date >= :hire_date", normalized,
                "Salary null; hire_date non null");


            // Salary not null; hire_date null
            _dictValues["salary"] = 1111;
            _dictValues["hire_date"] = null;
            queryOut = XmlToSql.BuildQuery(QUERY, _dictValues, _dictRepeats);
            usedParams = XmlToSql.GetParametersUsed(queryOut);
            normalized = Regex.Replace(queryOut, @"\s+", " ").Trim();

            Assert.AreEqual(1, usedParams.Length);
            Assert.IsTrue(usedParams.Contains("salary"));
            Assert.AreEqual("SELECT * FROM employees WHERE 1 = 1 AND salary >= :salary", normalized,
                "Salary not null; hire_date null");

            // Both not null
            _dictValues["salary"] = 1111;
            _dictValues["hire_date"] = 2222;
            queryOut = XmlToSql.BuildQuery(QUERY, _dictValues, _dictRepeats);
            usedParams = XmlToSql.GetParametersUsed(queryOut);
            normalized = Regex.Replace(queryOut, @"\s+", " ").Trim();
            Assert.AreEqual(2, usedParams.Length);
            Assert.IsTrue(usedParams.Contains("salary"));
            Assert.IsTrue(usedParams.Contains("hire_date"));
            Assert.AreEqual("SELECT * FROM employees WHERE 1 = 1 AND salary >= :salary AND hire_date >= :hire_date",
                normalized, "Both not null");
        }

    }
}
