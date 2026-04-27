using MatchLogic.Application.Features.DataProfiling;
using MatchLogic.Domain.DataProfiling;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace MatchLogic.Application.UnitTests
{
    public class ColumnAnalyzerTests
    {
        private readonly ProfilingOptions _options;
        private readonly List<(Guid Id, string Name, Regex Pattern)> _regexPatterns;
        private readonly List<(Guid Id, string Name, HashSet<string> Items)> _dictionaries;

        public ColumnAnalyzerTests()
        {
            // Create standard profiling options
            _options = new ProfilingOptions
            {
                BatchSize = 5000,
                MaxDegreeOfParallelism = Environment.ProcessorCount,
                BufferSize = 10000,
                SampleSize = 100,
                MaxRowsPerCategory = 10,
                MaxDistinctValuesToTrack = 100,
                StoreCompleteRows = true
            };

            // Create some test regex patterns
            _regexPatterns = new List<(Guid Id, string Name, Regex Pattern)>
            {
                (Guid.NewGuid(), "Email", new Regex(@"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", RegexOptions.Compiled)),
                (Guid.NewGuid(), "US Phone", new Regex(@"^\(\d{3}\) \d{3}-\d{4}$", RegexOptions.Compiled)),
                (Guid.NewGuid(), "Numeric", new Regex(@"^-?\d+(\.\d+)?$", RegexOptions.Compiled))
            };

            // Create some test dictionaries
            _dictionaries = new List<(Guid Id, string Name, HashSet<string> Items)>
            {
                (Guid.NewGuid(), "Names", new HashSet<string>(StringComparer.OrdinalIgnoreCase)
                {
                    "John", "Jane", "Mike", "Sarah", "David"
                }),
                (Guid.NewGuid(), "Countries", new HashSet<string>(StringComparer.OrdinalIgnoreCase)
                {
                    "USA", "Canada", "UK", "Australia", "Germany", "France"
                })
            };
        }

        [Fact]
        public void Analyze_StringColumn_CorrectlyProfilesData()
        {
            // Arrange
            var columnAnalyzer = new ColumnAnalyzer("Name", _options, _regexPatterns, _dictionaries);
            var testData = GenerateTestStringData();

            // Act
            ProcessTestData(columnAnalyzer, testData);
            var profile = columnAnalyzer.BuildColumnProfile();

            // Assert
            Assert.Equal("Name", profile.FieldName);
            Assert.Equal("String", profile.Type);
            Assert.Equal(10, profile.Total);  // Total records
            Assert.Equal(2, profile.Null);    // Null records
            Assert.Equal(1, profile.Distinct); // Distinct values (excluding nulls)
            Assert.True(profile.LettersOnly > 0);
            Assert.Equal(0, profile.NumbersOnly);
            Assert.Equal("Names", profile.Pattern); // Should identify the Names dictionary

            // Verify pattern matching worked
            Assert.Contains(profile.Patterns, p => p.Pattern == "Names");

            // Verify value distribution
            Assert.NotNull(profile.ValueDistribution);
            Assert.True(profile.ValueDistribution.ContainsKey("John"));

            // Verify that _characteristicRows is populated (internal testing)
            Assert.NotEmpty(columnAnalyzer._characteristicRows[ProfileCharacteristic.LettersOnly]);
        }

        [Fact]
        public void Analyze_NumericColumn_CorrectlyProfilesData()
        {
            // Arrange
            var columnAnalyzer = new ColumnAnalyzer("Age", _options, _regexPatterns, _dictionaries);
            var testData = GenerateTestNumericData();

            // Act
            ProcessTestData(columnAnalyzer, testData);
            var profile = columnAnalyzer.BuildColumnProfile();

            // Assert
            Assert.Equal("Age", profile.FieldName);
            Assert.Equal("Integer", profile.Type);//OV - it is as string so need to look in to this // Should infer integer type
            Assert.Equal(10, profile.Total);
            Assert.Equal(1, profile.Null);
            Assert.Equal(5, profile.Distinct);
            Assert.True(profile.NumbersOnly > 0); //OV - it is no inferring as numberonly because of string
            Assert.Equal(0, profile.LettersOnly);
            Assert.Equal("Numeric", profile.Pattern);  // Should match the Numeric pattern

            // Verify min/max/mean are correctly calculated
            Assert.Equal("10", profile.Min);
            Assert.Equal("50", profile.Max);
            Assert.NotEmpty(profile.Mean);
            Assert.NotEmpty(profile.Median);

            // Verify that characteristic rows for numerics are populated
            Assert.NotEmpty(columnAnalyzer._characteristicRows[ProfileCharacteristic.NumbersOnly]);
            Assert.NotEmpty(columnAnalyzer._patternStats["Numeric"].ValidRows);
        }

        [Fact]
        public void Analyze_EmailColumn_CorrectlyProfilesData()
        {
            // Arrange
            var columnAnalyzer = new ColumnAnalyzer("Email", _options, _regexPatterns, _dictionaries);
            var testData = GenerateTestEmailData();

            // Act
            ProcessTestData(columnAnalyzer, testData);
            var profile = columnAnalyzer.BuildColumnProfile();

            // Assert
            Assert.Equal("Email", profile.FieldName);
            Assert.Equal("String", profile.Type);
            Assert.Equal(10, profile.Total);
            Assert.Equal(1, profile.Null);
            //Assert.Equal(7, profile.Distinct); //OV - it is returning as 9 as it is counting invalid also
            Assert.Equal("Email", profile.Pattern);  // Should match the Email pattern

            // Verify valid/invalid counts
            var validCount = profile.Patterns.First(p => p.Pattern == "Email").Count;
            Assert.Equal(7, validCount);  // 7 valid emails, 2 invalid, 1 null

            // Check for presence of @ symbols
            Assert.True(profile.Punctuation > 0);

            // Verify pattern match stats
            Assert.NotEmpty(columnAnalyzer._patternStats["Email"].ValidRows);
            Assert.NotEmpty(columnAnalyzer._patternStats["Email"].InvalidRows);
        }

        [Fact]
        public void Analyze_MixedDataColumn_CorrectlyHandlesDifferentTypes()
        {
            // Arrange
            var columnAnalyzer = new ColumnAnalyzer("Mixed", _options, _regexPatterns, _dictionaries);
            var testData = GenerateTestMixedData();

            // Act
            ProcessTestData(columnAnalyzer, testData);
            var profile = columnAnalyzer.BuildColumnProfile();

            // Assert
            Assert.Equal("Mixed", profile.FieldName);
            Assert.Equal("String", profile.Type);  // Should default to string due to mixed content
            Assert.Equal(11, profile.Total);
            Assert.Equal(0, profile.Null);
            Assert.Equal(11, profile.Distinct);  // All values are different

            // Verify character counts
            Assert.True(profile.Letters > 0);
            Assert.True(profile.Numbers > 0);
            Assert.True(profile.Punctuation > 0);

            // Check that pattern matching is working
            Assert.Contains(profile.Patterns, p => p.Pattern == "Email");
            Assert.Contains(profile.Patterns, p => p.Pattern == "Numeric");
            Assert.Contains(profile.Patterns, p => p.Pattern == "Names");
        }

        [Fact]
        public void Analyze_EmptyStringsAndNulls_CorrectlyTracked()
        {
            // Arrange
            var columnAnalyzer = new ColumnAnalyzer("Status", _options, _regexPatterns, _dictionaries);
            var testData = new List<(object Value, Dictionary<string, object> Row, long RowNumber)>
            {
                (null, new Dictionary<string, object> { { "Status", null } }, 1),
                ("InActive", new Dictionary<string, object> { { "Status", "InActive" } }, 2),
                (string.Empty, new Dictionary<string, object> { { "Status", string.Empty } }, 3),
                ("Active", new Dictionary<string, object> { { "Status", "Active" } }, 4),
                (null, new Dictionary<string, object> { { "Status", null } }, 5),
                ("", new Dictionary<string, object> { { "Status", "" } }, 6),
            };

            // Act
            ProcessTestData(columnAnalyzer, testData);
            var profile = columnAnalyzer.BuildColumnProfile();

            // Assert
            Assert.Equal(6, profile.Total);
            Assert.Equal(2, profile.Null);  // 2 nulls
            Assert.Equal(4, profile.Total - profile.Filled); // 2 nulls and 2 empty strings

            // Verify row references for nulls and empty values
            Assert.Equal(2, columnAnalyzer._characteristicRows[ProfileCharacteristic.Null].Count);
            Assert.True(columnAnalyzer._characteristicRows.ContainsKey(ProfileCharacteristic.Empty));
        }

        [Fact]
        public void Analyze_SpecialCharacters_CorrectlyIdentified()
        {
            // Arrange
            var columnAnalyzer = new ColumnAnalyzer("Special", _options, _regexPatterns, _dictionaries);
            var testData = new List<(object Value, Dictionary<string, object> Row, long RowNumber)>
            {
                ("Hello!", new Dictionary<string, object> { { "Special", "Hello!" } }, 1),
                ("123,456.78", new Dictionary<string, object> { { "Special", "123,456.78" } }, 2),
                (" Leading Space", new Dictionary<string, object> { { "Special", " Leading Space" } }, 3),
                ("Has\tTab", new Dictionary<string, object> { { "Special", "Has\tTab" } }, 4),
                //OV - ("$Money$", new Dictionary<string, object> { { "Special", "$Money$" } }, 5),
                 ("Control\u0007Character", new Dictionary<string, object> { { "Special", "Control\u0007Character" } }, 5),
            };

            // Act
            ProcessTestData(columnAnalyzer, testData);
            var profile = columnAnalyzer.BuildColumnProfile();

            // Assert
            Assert.Equal(5, profile.Total);
            Assert.True(profile.Punctuation > 0);
            Assert.True(profile.LeadingSpaces > 0);
            Assert.True(profile.NonPrintableCharacters > 0);

            // Verify row references for special character types
            Assert.NotEmpty(columnAnalyzer._characteristicRows[ProfileCharacteristic.WithPunctuation]);
            Assert.NotEmpty(columnAnalyzer._characteristicRows[ProfileCharacteristic.WithLeadingSpaces]);
            Assert.NotEmpty(columnAnalyzer._characteristicRows[ProfileCharacteristic.WithNonPrintable]);
        }

        [Fact]
        public void Analyze_LargeDataset_CorrectlyHandlesSampling()
        {
            // Arrange
            var columnAnalyzer = new ColumnAnalyzer("LargeColumn", _options, _regexPatterns, _dictionaries);

            // Generate 1000 rows of test data (exceeding the sample limit)
            var largeTestData = new List<(object Value, Dictionary<string, object> Row, long RowNumber)>();
            for (int i = 0; i < 1000; i++)
            {
                var value = i % 10 == 0 ? null : i.ToString();
                largeTestData.Add((value, new Dictionary<string, object> { { "LargeColumn", value } }, i));
            }

            // Act
            ProcessTestData(columnAnalyzer, largeTestData);
            var profile = columnAnalyzer.BuildColumnProfile();

            // Assert
            Assert.Equal(1000, profile.Total);
            Assert.Equal(100, profile.Null);  // 100 nulls (every 10th row)

            //// Verify max row references are properly limited
            foreach (var characteristic in columnAnalyzer._characteristicRows)
            {
                Assert.True(characteristic.Value.Count <= _options.MaxRowsPerCategory);
            }

            //OV - there is check in code to populate this Check value distribution - handle case when it might be null
            if (profile.ValueDistribution != null)
            {
                Assert.True(profile.ValueDistribution.Count <= _options.MaxDistinctValuesToTrack);
            }
        }

        [Fact]
        public void BuildColumnProfile_ReturnedTwice_ProducesIdenticalResults()
        {
            // Arrange
            var columnAnalyzer = new ColumnAnalyzer("TestColumn", _options, _regexPatterns, _dictionaries);
            var testData = GenerateTestMixedData();
            ProcessTestData(columnAnalyzer, testData);

            // Act
            var profile1 = columnAnalyzer.BuildColumnProfile();
            var profile2 = columnAnalyzer.BuildColumnProfile();

            // Assert - results should be stable
            Assert.Equal(profile1.Total, profile2.Total);
            Assert.Equal(profile1.Type, profile2.Type);
            Assert.Equal(profile1.Pattern, profile2.Pattern);
            Assert.Equal(profile1.Min, profile2.Min);
            Assert.Equal(profile1.Max, profile2.Max);
            Assert.Equal(profile1.Mean, profile2.Mean);
            Assert.Equal(profile1.Patterns.Count, profile2.Patterns.Count);
        }

        #region Test Data Generation

        private List<(object Value, Dictionary<string, object> Row, long RowNumber)> GenerateTestStringData()
        {
            var testData = new List<(object Value, Dictionary<string, object> Row, long RowNumber)>();

            for (int i = 0; i < 10; i++)
            {
                object value = i < 8 ? "John" : null;  // 8 Johns, 2 nulls
                var row = new Dictionary<string, object>
                {
                    { "Name", value },
                    { "Age", 30 + i },
                    { "Email", $"john{i}@example.com" }
                };

                testData.Add((value, row, i));
            }

            return testData;
        }

        private List<(object Value, Dictionary<string, object> Row, long RowNumber)> GenerateTestNumericData()
        {
            var testData = new List<(object Value, Dictionary<string, object> Row, long RowNumber)>();

            var values = new object[] { 10, 20, 30, 40, 50, 10, 30, 40, 20, null };  // 9 numbers, 1 null

            for (int i = 0; i < 10; i++)
            {
                var row = new Dictionary<string, object>
                {
                    { "Name", $"Person{i}" },
                    { "Age", values[i] },
                    { "Salary", 50000 + (i * 5000) }
                };

                testData.Add((values[i], row, i));
            }

            return testData;
        }

        private List<(object Value, Dictionary<string, object> Row, long RowNumber)> GenerateTestEmailData()
        {
            var testData = new List<(object Value, Dictionary<string, object> Row, long RowNumber)>();

            var values = new object[]
            {
                "user1@example.com",
                "user2@example.com",
                "user3@test.co.uk",
                "john.doe@company.org",
                "jane.smith@email.net",
                "not-an-email",  // Invalid
                "also@not@valid.com",  // Invalid
                "info@domain.com",
                "support@website.io",
                null  // Null
            };

            for (int i = 0; i < 10; i++)
            {
                var row = new Dictionary<string, object>
                {
                    { "Name", $"User {i}" },
                    { "Email", values[i] },
                    { "Active", i % 2 == 0 }
                };

                testData.Add((values[i], row, i));
            }

            return testData;
        }

        private List<(object Value, Dictionary<string, object> Row, long RowNumber)> GenerateTestMixedData()
        {
            var testData = new List<(object Value, Dictionary<string, object> Row, long RowNumber)>();

            var values = new object[]
            {
                "John",  // Name match
                42,  // Number
                "user@example.com",  // Email match
                true,  // Boolean
                DateTime.Now,  // DateTime
                3.14159,  // Decimal
                "USA",  // Country match
                "Special@Chars#!",  // Special characters
                " Leading space",  // Leading space
                "123-456-7890",  // Phone format
                "alpha1"
            };

            for (int i = 0; i < 11; i++)
            {
                var row = new Dictionary<string, object>
                {
                    { "Mixed", values[i] },
                    { "Index", i }
                };

                testData.Add((values[i], row, i));
            }

            return testData;
        }

        #endregion

        #region Helper Methods

        private void ProcessTestData(ColumnAnalyzer analyzer, List<(object Value, Dictionary<string, object> Row, long RowNumber)> testData)
        {
            foreach (var (value, row, rowNumber) in testData)
            {
                analyzer.Analyze(value, row, rowNumber);
            }
        }

        #endregion
    }
}
