using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using HappyOracle.WellManagedDataAccess.Client;
using HappyOracle.WellManagedDataAccess.Helpers;
using System.Collections.Generic;
using System.Linq;

namespace WellManagedDataAccessUnitTests
{
    [TestClass]
    public class SelectQueryUnitTests
    {

        private const string CONNECT_STRING = "User Id=scott; Password=tiger; Data Source=localhost/dcmsprd1";

        private static WConnection __db = new WConnection(CONNECT_STRING);

        public class Employee
        {
            public int? EmployeeId { get; set; }
            public string FirstName { get; set; }
            public string LastName { get; set; }
            public string Email { get; set; }
            public string PhoneNumber { get; set; }
            public DateTime? HireDate { get; set; }
            public decimal? Salary { get; set; }
            public decimal? CommisionPct { get; set; }
        }

        private static WCommand<Employee> __binderEmployee;

        private static IList<Employee> __stdEmployees;

        /// <summary>
        /// Executes before the start of the first test. Drop and recreate the DEPARTMENTS
        /// </summary>
        /// <param name="ctx"></param>
        [ClassInitialize]
        public static void Initialize(TestContext ctx)
        {
            try
            {
                __db.ExecuteNonQuery(@"drop table EMPLOYEES cascade constraints");
            }
            catch (WOracleException ex) when (ex.OracleErrorNumber == OracleErrorNumber.TABLE_OR_VIEW_DOES_NOT_EXIST)
            {
                // No such table. Do nothing
            }
            __db.ExecuteNonQuery(@"
create table EMPLOYEES
(
  employee_id    NUMBER(6) not null,
  first_name     VARCHAR2(20),
  last_name      VARCHAR2(25),
  email          VARCHAR2(25),
  phone_number   VARCHAR2(20),
  hire_date      DATE,
  job_id         VARCHAR2(10),
  salary         NUMBER(8,2),
  commission_pct NUMBER(2,2),
  manager_id     NUMBER(6),
  department_id  NUMBER(4)
)
");
            const string SCRIPT_INSERT = @"
insert into EMPLOYEES (employee_id, first_name, last_name, email, phone_number, hire_date, job_id, salary, commission_pct, manager_id, department_id)
values (100, 'Steven', 'King', 'SKING', '515.123.4567', to_date('17-06-1987', 'dd-mm-yyyy'), 'AD_PRES', 24000, null, null, 90);
insert into EMPLOYEES (employee_id, first_name, last_name, email, phone_number, hire_date, job_id, salary, commission_pct, manager_id, department_id)
values (101, 'Neena', 'Kochhar', 'NKOCHHAR', '515.123.4568', to_date('21-09-1989', 'dd-mm-yyyy'), 'AD_VP', 17000, null, 100, 90);
insert into EMPLOYEES (employee_id, first_name, last_name, email, phone_number, hire_date, job_id, salary, commission_pct, manager_id, department_id)
values (102, 'Lex', 'De Haan', 'LDEHAAN', '515.123.4569', to_date('13-01-1993', 'dd-mm-yyyy'), 'AD_VP', 17000, null, 100, 90);
insert into EMPLOYEES (employee_id, first_name, last_name, email, phone_number, hire_date, job_id, salary, commission_pct, manager_id, department_id)
values (103, 'Alexander', 'Hunold', 'AHUNOLD', '590.423.4567', to_date('03-01-1990', 'dd-mm-yyyy'), 'IT_PROG', 9000, null, 102, 60);
insert into EMPLOYEES (employee_id, first_name, last_name, email, phone_number, hire_date, job_id, salary, commission_pct, manager_id, department_id)
values (104, 'Bruce', 'Ernst', 'BERNST', '590.423.4568', to_date('21-05-1991', 'dd-mm-yyyy'), 'IT_PROG', 6000, null, 103, 60);
insert into EMPLOYEES (employee_id, first_name, last_name, email, phone_number, hire_date, job_id, salary, commission_pct, manager_id, department_id)
values (105, 'David', 'Austin', 'DAUSTIN', '590.423.4569', to_date('25-06-1997', 'dd-mm-yyyy'), 'IT_PROG', 4800, null, 103, 60);
insert into EMPLOYEES (employee_id, first_name, last_name, email, phone_number, hire_date, job_id, salary, commission_pct, manager_id, department_id)
values (106, 'Valli', 'Pataballa', 'VPATABAL', '590.423.4560', to_date('05-02-1998', 'dd-mm-yyyy'), 'IT_PROG', 4800, null, 103, 60);
insert into EMPLOYEES (employee_id, first_name, last_name, email, phone_number, hire_date, job_id, salary, commission_pct, manager_id, department_id)
values (107, 'Diana', 'Lorentz', 'DLORENTZ', '590.423.5567', to_date('07-02-1999', 'dd-mm-yyyy'), 'IT_PROG', 4200, null, 103, 60);
insert into EMPLOYEES (employee_id, first_name, last_name, email, phone_number, hire_date, job_id, salary, commission_pct, manager_id, department_id)
values (108, 'Nancy', 'Greenberg', 'NGREENBE', '515.124.4569', to_date('17-08-1994', 'dd-mm-yyyy'), 'FI_MGR', 12000, null, 101, 100);
insert into EMPLOYEES (employee_id, first_name, last_name, email, phone_number, hire_date, job_id, salary, commission_pct, manager_id, department_id)
values (109, 'Daniel', 'Faviet', 'DFAVIET', '515.124.4169', to_date('16-08-1994', 'dd-mm-yyyy'), 'FI_ACCOUNT', 9000, null, 108, 100);
insert into EMPLOYEES (employee_id, first_name, last_name, email, phone_number, hire_date, job_id, salary, commission_pct, manager_id, department_id)
values (110, 'John', 'Chen', 'JCHEN', '515.124.4269', to_date('28-09-1997', 'dd-mm-yyyy'), 'FI_ACCOUNT', 8200, null, 108, 100);
";
            var queries = SCRIPT_INSERT.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries);
            foreach (var query in queries)
            {
                if (!string.IsNullOrWhiteSpace(query))
                {
                    __db.ExecuteNonQuery(query);
                }
            }

            __binderEmployee = WCommand.Create(@"
select employee_id, first_name, last_name, email, phone_number, hire_date, salary, commission_pct
  from employees
", row => new Employee
            {
                EmployeeId = row.GetInteger("employee_id"),
                CommisionPct = row.GetDecimal("commission_pct"),
                Email = row.GetString("email"),
                FirstName = row.GetString("first_name"),
                HireDate = row.GetDate("hire_date"),
                LastName = row.GetString("last_name"),
                PhoneNumber = row.GetString("phone_number"),
                Salary = row.GetDecimal("salary")
            });

            __stdEmployees = __binderEmployee.ExecuteReader(__db);
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



        [TestMethod]
        public void SelectQuery_MultipleIfTags()
        {
            const string QUERY = @"
<query>
SELECT employee_id, first_name, last_name, email, phone_number, hire_date, salary, commission_pct
  FROM employees
 WHERE 1 = 1
<if>AND salary &gt;= :salary</if>
<if>AND hire_date &gt;= :hire_date</if>
</query>
";
            decimal? salary = null;
            DateTime? hireDate = null;

            __binderEmployee.Parameter("salary", salary)
                .Parameter("hire_date", hireDate);
            __binderEmployee.CommandText = QUERY;
            // Both parameters null. All employees retrieved
            var list = __binderEmployee.ExecuteReader(__db);
            Assert.AreEqual(__stdEmployees.Count, list.Count, "count retrieved should match count of all employees");

            // only salary specified
            salary = 10000;
            hireDate = null;

            __binderEmployee.Parameter("salary", salary)
                .Parameter("hire_date", hireDate);
            __binderEmployee.CommandText = QUERY;
            list = __binderEmployee.ExecuteReader(__db);
            Assert.AreEqual(__stdEmployees.Where(p => p.Salary >= salary).Count(),
                list.Count,
                "count retrieved should match count of all employees with specified salary or higher");

            // only hire date specified
            salary = null;
            hireDate = new DateTime(1993, 1, 1);

            __binderEmployee.Parameter("salary", salary)
                .Parameter("hire_date", hireDate);
            __binderEmployee.CommandText = QUERY;
            list = __binderEmployee.ExecuteReader(__db);
            Assert.AreEqual(__stdEmployees.Where(p => p.HireDate >= hireDate).Count(),
                list.Count,
                "count retrieved should match count of all employees hired after specified hire date");

            // both hire date and salary specified
            salary = 10000;
            hireDate = new DateTime(1993, 1, 1);

            __binderEmployee.Parameter("salary", salary)
                .Parameter("hire_date", hireDate);
            __binderEmployee.CommandText = QUERY;
            list = __binderEmployee.ExecuteReader(__db);
            Assert.AreEqual(__stdEmployees.Where(p => p.HireDate >= hireDate && p.Salary >= salary).Count(),
                list.Count,
                "count retrieved should match count of all employees hired after specified hire date having at least salary specified");
        }

        [TestMethod]
        public void SelectQuery_StringParameter()
        {
            const string QUERY = @"
<query>
SELECT employee_id, first_name, last_name, email, phone_number, hire_date, salary, commission_pct
  FROM employees
 WHERE first_name LIKE :first_name || '%'
</query>
";

            var firstName = "S";
            __binderEmployee.Parameter("first_name", firstName);
            __binderEmployee.CommandText = QUERY;
            var list = __binderEmployee.ExecuteReader(__db);
            Assert.AreEqual(__stdEmployees.Where(p => p.FirstName.StartsWith(firstName)).Count(),
                list.Count,
                "count retrieved should match count of all employees with specified first name");
        }

        [TestMethod]
        public void SelectQuery_IntParameter()
        {
            const string QUERY = @"
SELECT employee_id, first_name, last_name, email, phone_number, hire_date, salary, commission_pct
  FROM employees
 WHERE employee_id <= :employee_id
";

            int employeeId = 103;
            __binderEmployee.Parameter("employee_id", employeeId);
            __binderEmployee.CommandText = QUERY;
            var list = __binderEmployee.ExecuteReader(__db);
            Assert.AreEqual(__stdEmployees.Where(p => p.EmployeeId <= employeeId).Count(),
                list.Count,
                "count retrieved should match count of all employees with specified employeeId");

            int? employeeIdNullable = 101;
            __binderEmployee.Parameter("employee_id", employeeIdNullable);
            list = __binderEmployee.ExecuteReader(__db);
            Assert.AreEqual(__stdEmployees.Where(p => p.EmployeeId <= employeeIdNullable).Count(),
                list.Count,
                "count retrieved should match count of all employees with specified employeeIdNullable");
        }

        /// <summary>
        /// Query retrieves the null value passed as parameter to binder
        /// </summary>
        [TestMethod]
        public void SelectQuery_IntParameterNullValue()
        {
            const string QUERY = @"
SELECT :employee_id AS employee_id, first_name, last_name, email, phone_number, hire_date, salary, commission_pct
  FROM employees
 WHERE rownum < 2
";

            int? employeeIdNullable = null;
            __binderEmployee.Parameter("employee_id", employeeIdNullable);
            __binderEmployee.CommandText = QUERY;
            var list = __binderEmployee.ExecuteReader(__db);
            Assert.AreEqual(1, list.Count,
                "count retrieved dictated by rownum");
            Assert.IsNull(list.First().EmployeeId, "Parameter value should be retrieved");
        }


        [TestMethod]
        public void SelectQuery_DateTimeParameter()
        {
            const string QUERY = @"
SELECT employee_id, first_name, last_name, email, phone_number, hire_date, salary, commission_pct
  FROM employees
 WHERE hire_date >= :hire_date
";

            var hireDate = new DateTime(1993, 1, 1);
            __binderEmployee.Parameter("hire_date", hireDate);
            __binderEmployee.CommandText = QUERY;
            var list = __binderEmployee.ExecuteReader(__db);
            Assert.AreEqual(__stdEmployees.Where(p => p.HireDate >= hireDate).Count(),
                list.Count,
                "count retrieved should match count of all employees hired after specified hire date");
        }

        [TestMethod]
        public void SelectQuery_DateParameterNullValue()
        {
            const string QUERY = @"
SELECT employee_id, first_name, last_name, email, phone_number, :hire_date as hire_date, salary, commission_pct
  FROM employees
 WHERE rownum < 2
";

            DateTime? hireDateNull = null;
            __binderEmployee.Parameter("hire_date", hireDateNull);
            __binderEmployee.CommandText = QUERY;
            var list = __binderEmployee.ExecuteReader(__db);
            Assert.AreEqual(1, list.Count,
                "count retrieved dictated by rownum");
            Assert.IsNull(list.First().HireDate, "Passed parameter value is retrieved");
        }

        [TestMethod]
        public void SelectQuery_DateTimeOffsetParameter()
        {
            const string QUERY = @"
SELECT :date_input
     AT TIME ZONE '-08:00' AS west_coast_time
FROM DUAL
            ";

            DateTimeOffset dateInput = DateTimeOffset.Now;
            var binder = WCommand.Create(QUERY, row => row.GetDateTimeOffset("west_coast_time"))
                .Parameter("date_input", dateInput);

            var dateOutput = binder.ExecuteSingle(__db);

            Assert.IsNotNull(dateOutput);
            Assert.AreEqual(dateInput.UtcDateTime, dateOutput,
                "input and output values should represent the same UTC time");
            Assert.AreNotEqual(dateInput.DateTime, dateOutput.Value.DateTime,
                "input and output values should represent the same UTC time");
            //Assert.Inconclusive(dateOutput.Value.ToString());
        }

        [TestMethod]
        public void SelectQuery_DecimalParameter()
        {
            const string QUERY = @"
SELECT employee_id, first_name, last_name, email, phone_number, hire_date, salary, commission_pct
  FROM employees
 WHERE salary >= :salary
";

            decimal salary = 10000;
            __binderEmployee.Parameter("salary", salary);
            __binderEmployee.CommandText = QUERY;
            var list = __binderEmployee.ExecuteReader(__db);
            Assert.AreEqual(__stdEmployees.Where(p => p.Salary >= salary).Count(),
                list.Count,
                "count retrieved should match count of all employees of specified salary");
        }

        [TestMethod]
        public void SelectQuery_LongParameter()
        {
            const string QUERY = @"
SELECT :long_input as long_output
FROM DUAL
            ";

            var longInput = long.MaxValue;
            var binder = WCommand.Create(QUERY, row => row.GetLong("long_output"))
                .Parameter("long_input", longInput);

            var longOutput = binder.ExecuteSingle(__db);

            Assert.AreEqual(longInput, longOutput,
                "input and output values should be same");
        }

        [TestMethod]
        public void SelectQuery_IntervalDSParameter()
        {
            const string QUERY = @"
select :interval_input + INTERVAL '1' DAY AS interval_output
FROM DUAL
            ";

            var interval_input = TimeSpan.FromHours(52);
            var binder = WCommand.Create(QUERY, row => row.GetTimeSpan("interval_output"))
                .Parameter("interval_input", interval_input);

            var interval_output = binder.ExecuteSingle(__db);

            Assert.AreEqual(interval_input.Add(TimeSpan.FromDays(1)), interval_output,
                "output should be one 1 day more than input");
        }

        [TestMethod]
        public void SelectQuery_IntervalYMColumn()
        {
            const string QUERY = @"
select INTERVAL '123-2' YEAR(3) TO MONTH
FROM DUAL
            ";

            //var interval_input = TimeSpan.FromHours(52);
            var binder = WCommand.Create(QUERY, row => row.GetLong(0));

            var interval_output = binder.ExecuteSingle(__db);

            Assert.AreEqual(123 * 12 + 2, interval_output);
            // Assert.Inconclusive(interval_output.ToString());
        }

        [TestMethod]
        public void TestMethod1()
        {
            //var cmd = WCommand.Create("select col1, col2 from t", row => new
            //{
            //    MyString = row.GetString("col1"),
            //    MyInt = row.GetInteger("col2")
            //});

            var cmd = WCommand.Create("delete from t where id = :id", 10);
            

        }
    }
}
