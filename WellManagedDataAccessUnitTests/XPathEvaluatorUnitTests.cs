using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using HappyOracle.WellManagedDataAccess.XPath;

namespace WellManagedDataAccessUnitTests
{
    [TestClass]
    public class XPathEvaluatorUnitTests
    {
        static readonly IDictionary<string, object> __dict = new Dictionary<string, object>
        {
            ["int0"] = 0,
            ["int1"] = 1,
            ["int2"] = 2,
            ["int3"] = 3,
            ["strEmpty"] = "",
            ["str1"] = "1",
            ["str2"] = "2",
            ["str3"] = "3",
        };

        static readonly XPathEvaluator _evaluator = new XPathEvaluator(__dict);


        [TestMethod]
        public void XPath_NullDictionaryNoVariables()
        {
            var eval = new XPathEvaluator(null);
            Assert.IsFalse(eval.Matches("0"),"0 should be false");
            Assert.IsTrue(eval.Matches("1"), "1 should be true");
            //Assert.IsTrue(eval.Matches("$int2"));
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void XPath_NullDictionaryWithVariables()
        {
            var eval = new XPathEvaluator(null);
            eval.Matches("$x");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void XPath_UnknownVariable()
        {
            Assert.IsTrue(_evaluator.Matches("$a"));
        }

        [TestMethod]
        public void XPath_IntegersAsBool()
        {
            Assert.IsTrue(_evaluator.Matches("$int1"));
            Assert.IsTrue(_evaluator.Matches("$int2"));
            Assert.IsTrue(_evaluator.Matches("$int3"));
            Assert.IsFalse(_evaluator.Matches("$int0"));
        }

        [TestMethod]
        public void XPath_StringsAsBool()
        {
            Assert.IsTrue(_evaluator.Matches("$str1"));
            Assert.IsTrue(_evaluator.Matches("$str2"));
            Assert.IsTrue(_evaluator.Matches("$str3"));
            Assert.IsFalse(_evaluator.Matches("$strEmpty"));
        }

        [TestMethod]
        public void XPath_LogicalExpressions()
        {
            var exprTrue = new[]
            {
                "$str1 and $str2",
                "$str1 and $int1",
                "$int2 and $int3"
            };

            var exprFalse = new[]
{
                "$str1 and not($str2)",
                "not($str1 and $int1)",
                "$int2 and $int0",
                "$str2 and $strEmpty"
            };

            foreach (var item in exprTrue)
            {
                Assert.IsTrue(_evaluator.Matches(item), item);
            }

            foreach (var item in exprFalse)
            {
                Assert.IsFalse(_evaluator.Matches(item), item);
            }
        }

        [TestMethod]
        public void XPath_RelationalExpressions()
        {
            var exprTrue = new[]
            {
                "$str1 = 1",  // string 1 matches number 1
                "$str1 = '1'",  // string 1 matches string 1
                "$int2 < $int3",
                "$int2 < $str3",        // integer compared to string
                "$int0 != $strEmpty",   // 0 does not match empty string
            };

            foreach (var item in exprTrue)
            {
                Assert.IsTrue(_evaluator.Matches(item), item);
            }
        }
    }
}
