using HappyOracle.WellManagedDataAccess.Client;
using HappyOracle.WellManagedDataAccess.Helpers;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Linq;
using System.Text;

namespace WellManagedDataAccessUnitTests
{
    [TestClass]
    public class LobUnitTests
    {
        private const string CONNECT_STRING = "User Id=scott; Password=tiger; Data Source=localhost/dcmsprd1";

        private static WConnection __db = new WConnection(CONNECT_STRING);

        private static string __lobValueHexString = "656667686970717273747576777879808182838485";

        private static byte[] __lobValueBytes;

        /// <summary>
        /// Executes before the start of the first test. Drop and recreate the DEPARTMENTS
        /// </summary>
        /// <param name="ctx"></param>
        [ClassInitialize]
        public static void Initialize(TestContext ctx)
        {
            try
            {
                __db.ExecuteNonQuery(@"DROP TABLE multimedia_tab");
            }
            catch (WOracleException ex) when (ex.OracleErrorNumber == OracleErrorNumber.TABLE_OR_VIEW_DOES_NOT_EXIST)
            {
                // No such table. Do nothing
            }

            __db.ExecuteNonQuery(@"
create table MULTIMEDIA_TAB
(
  thekey NUMBER(4) not null,
  story  CLOB,
  sound  BLOB
)
");
            __db.ExecuteNonQuery(@"
alter table MULTIMEDIA_TAB
  add primary key (THEKEY)
");


            // Hex string to byte array
            // http://stackoverflow.com/questions/321370/how-can-i-convert-a-hex-string-to-a-byte-array
            __lobValueBytes = Enumerable.Range(0, __lobValueHexString.Length)
                     .Where(x => x % 2 == 0)
                     .Select(x => Convert.ToByte(__lobValueHexString.Substring(x, 2), 16))
                     .ToArray();
        }

        [TestInitialize]
        public void TestInitialize()
        {
            __db.ExecuteNonQuery(@"truncate table multimedia_tab");
            __db.ExecuteNonQuery($@"
INSERT INTO multimedia_tab values(1,
'This is a long story. Once upon a time ...', '{__lobValueHexString}')");
        }


        [TestMethod]
        public void Lob_LobOutParameter()
        {
            const string QUERY = @"
begin
  select sound into :1 from multimedia_tab where thekey = 1;
end;
";
            byte[] blobOut = null;
            var cmd = WCommand.Create(QUERY)
                .OutParameter("1", val => blobOut = val);

            var nRows = cmd.ExecuteNonQuery(__db);

            Assert.IsNotNull(blobOut);
            StringBuilder hex = new StringBuilder(blobOut.Length * 2);
            foreach (byte b in blobOut)
            {
                hex.AppendFormat("{0:x2}", b);
            }
            Assert.AreEqual(__lobValueHexString, hex.ToString());
        }

        [TestMethod]
        public void Lob_UpdateLobToNull()
        {
            const string QUERY = @"
  update multimedia_tab set sound = :lob_in where thekey = 1
RETURNING sound into :lob_out
";
            byte[] lobOut = new byte[2];  // Query will make it null
            var cmd = WCommand.Create(QUERY)
                .Parameter("lob_in", (byte[])null)
                .OutParameter("lob_out", val => lobOut = val);

            cmd.ExecuteNonQuery(__db);
            Assert.IsNull(lobOut);
        }

        [TestMethod]
        public void Lob_UpdateLobToNonNull()
        {
            const string QUERY = @"
  update multimedia_tab set sound = :lob_in where thekey = 1
RETURNING sound into :lob_out
";
            string str = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";
            // String to byte array
            // http://stackoverflow.com/questions/472906/converting-a-string-to-byte-array-without-using-an-encoding-byte-by-byte
            byte[] lobIn = new byte[str.Length * sizeof(char)];
            Buffer.BlockCopy(str.ToCharArray(), 0, lobIn, 0, lobIn.Length);

            byte[] lobOut = null;
            var cmd = WCommand.Create(QUERY)
                .Parameter("lob_in", lobIn)
                .OutParameter("lob_out", val => lobOut = val);

            cmd.ExecuteNonQuery(__db);

            Assert.IsTrue(lobIn.SequenceEqual(lobOut));
        }

        [TestMethod]
        public void Lob_SelectLob()
        {
            const string QUERY = @"
  select sound from multimedia_tab where thekey = 1
";

            var cmd = WCommand.Create(QUERY, row => row.GetBlob(0));

            var lobOut = cmd.ExecuteSingle(__db);

            Assert.IsTrue(__lobValueBytes.SequenceEqual(lobOut));
        }
    }
}
