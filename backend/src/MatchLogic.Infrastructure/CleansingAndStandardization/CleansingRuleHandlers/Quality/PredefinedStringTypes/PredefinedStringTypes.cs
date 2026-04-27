using System;
using System.Collections.Generic;
using System.IO;
using System.Data;
using Microsoft.VisualBasic.FileIO;

using MatchLogic.EncryptDecrypt;
using MatchLogic.Compression;

using MatchLogic.Core.Security;
using MatchLogic.Infrastructure.CleansingAndStandardization.Parser.AddressParser;
using System.Linq;
using System.Threading.Tasks;
using MatchLogic.Infrastructure;

// do not encrypt

namespace MatchLogic.Data.QualityII;

/// <summary>
/// Manages loading the predefined dictionaries of textual data.
/// </summary>
public class PredefinedStringTypes
{
    #region Constants
    private readonly object _loadLock = new object();
    private bool _isLoaded = false; // ADD this flag

    private IReadOnlyDictionary<string, string> _commonNamesDictionary;
    private const string CommonNamesResourceName = "MatchLogic.Infrastructure.Resources.CommonNames.xml";
    /// <summary>
    /// Name of the dictionary of first names.
    /// </summary>
    public const String FirstNameDictionaryDesription = "First name";
    /// <summary>
    /// Name of the dictionary of last names.
    /// </summary>
    public const String LastNameDictionaryDesription = "Last name";
    /// <summary>
    /// Name of the dictionary of cities.
    /// </summary>
    public const String CityDictionaryDescription = "City";
    /// <summary>
    /// Name of the dictionary of states.
    /// </summary>
    public const String StateDictionaryDescription = "State";

    #endregion

    #region Properties and Fields

    //private static readonly ILog Log = LogManager.GetLogger(typeof(PredefinedStringTypes));

    // key is the word(s) from predefined dictionary and values are all dictionaries which contain that word(s)...
    // |word              | description of type where word belongs
    private readonly Dictionary<String, Dictionary<String, DictionaryTypeGuesser>> _value2GuessersDictionary =
        new Dictionary<String, Dictionary<String, DictionaryTypeGuesser>>();

    /// <summary>
    /// Serves for the determination of types of string data.
    /// </summary>
    //public StringDataTypeDeterminator StringDataTypeDeterminator { get; set; }

    /// <summary>
    /// Holds the options of the dictionaries.
    /// </summary>
    public ProfilerDictionaryOptions DictionaryOptions { get; set; }

    #endregion

    #region Events

    /// <summary>
    /// Fired when dictionaris are starting loading.
    /// </summary>
    public event EventHandler<ProfilerDictionariesEventArgs> BeforeLoadingProfilerDictionaries;
    
    /// <summary>
    /// Fired when a dictionary has just been loaded.
    /// </summary>
    public event EventHandler<LoadingProfilerDictionariesEventArgs> LoadingProfilerDictionaries;

    /// <summary>
    /// Fires the BeforeLoadingProfilerDictionaries event. Call it before loading data dictionaries.
    /// </summary>
    /// <param name="dictionaryCount">Number of dictionaries to load.</param>
    protected void OnBeforeLoadingProfilerDictionaries(Int32 dictionaryCount)
    {
        BeforeLoadingProfilerDictionaries?.Invoke(this, new ProfilerDictionariesEventArgs() { DictionaryCount = dictionaryCount });
    }

    /// <summary>
    /// Fires the LoadingProfilerDictionaries event. Call it right after a dictionary was loaded.
    /// </summary>
    /// <param name="dictionaryDescription">Description of the dictionary.</param>
    protected void OnLoadingProfilerDictionaries(String dictionaryDescription)
    {
        LoadingProfilerDictionaries?.Invoke(this, new LoadingProfilerDictionariesEventArgs() { DictionaryDescription = dictionaryDescription });
    }

    #endregion

    #region Methods

    /// <summary>
    /// Loads the geographical dictionaries.
    /// </summary>
    /// <param name="addressParser">Address parser that posesses state abbreviations.</param>
    public void LoadGeoDictionaries(AddressParser addressParser)
    {
        if (_value2GuessersDictionary.Count != 0) return;

        LoadStates(addressParser);
        LoadGeoData();
        LoadUserDefinedDictionaries();
    }

    /// <summary>
    /// Loads the name dictionaries (common names, last names).
    /// </summary>
    public void LoadNameDictionaries()
    {
        lock (_loadLock)
        {
            if (_isLoaded) return; // Double-check after lock

            if (_value2GuessersDictionary.Count != 0) return; // Original check

            LoadCommonNames();
            LoadLastNames();

            _isLoaded = true; // Mark as loaded
        }
    }

    /// <summary>
    /// Loads all the dictionaries.
    /// </summary>
    public void Load()
    {
        if (_value2GuessersDictionary.Count != 0) return;

        OnBeforeLoadingProfilerDictionaries(5); // five dictionaries to load

        AddressParser addressParser = new AddressParser();
        LoadStates(addressParser);
        LoadCommonNames();
        LoadLastNames();
        LoadGeoData();
        LoadUserDefinedDictionaries();
    }

    private void LoadStates(AddressParser addressParser)
    {
        if (!(DictionaryOptions?.IsEnabled(StateDictionaryDescription) ?? false)) return;

        foreach (KeyValuePair<String, String> keyValuePair in addressParser.StatePossessionAbbreviations.Dictionary)
        {
            AddTypeGuesser(StateDictionaryDescription, keyValuePair.Key, false);
        }

        OnLoadingProfilerDictionaries("States");
    }

    private MemoryStream ReadAndDecompressFile(String filePath)
    {
        using (FileStream encryptedCompressedStream = new FileStream(filePath, FileMode.Open, FileAccess.Read))
        using (MemoryStream decryptedCompressedStream = EncryptDecryptString.DecryptStream(encryptedCompressedStream))
        {
            return  CompressWrapper.Decompress(decryptedCompressedStream);
        }
    }

    // this file contains geographic names (towns, locations, countries...)
    private void LoadGeoData()
    {
        if (!DictionaryOptions?.IsEnabled(CityDictionaryDescription) ?? false)
        {
            return;
        }

        Int32 recordCounter = 0;
        Boolean allowTextSplit = false;

        String resourceName = "MatchLogic.Infrastructure.Resources.populated places.archive";

        var assembly = System.Reflection.Assembly.GetExecutingAssembly();

        OnProgressChanged?.Invoke("Opening dictionary...");
        
        using (Stream encryptedCompressedStream = assembly.GetManifestResourceStream(resourceName))
        using (MemoryStream decryptedCompressedStream = EncryptDecryptString.DecryptStream(encryptedCompressedStream))
        using (MemoryStream decompressedStream = CompressWrapper.Decompress(decryptedCompressedStream))
        using (StreamReader streamReader = new StreamReader(decompressedStream))
        using (TextFieldParser reader = new TextFieldParser(streamReader))
        {
            reader.Delimiters = new[] { "\t" };
            reader.CommentTokens = new[] { "#" };
            reader.HasFieldsEnclosedInQuotes = false;

            if (reader.EndOfData)
            {
                return;
            }

            String[] values = reader.ReadFields();

            while (!reader.EndOfData)
            {
                if (recordCounter % 1000 == 0)
                {
                    OnProgressChanged?.Invoke($"Dictionary records read: {recordCounter}");
                }

                recordCounter++;

                values = reader.ReadFields();
                String name = (values?.Length ?? 0) > 0 ? values[0] : String.Empty;

                if (!String.IsNullOrEmpty(name))
                {
                    AddTypeGuesser(CityDictionaryDescription, name, allowTextSplit);

                    if (name.Contains("Saint "))
                    {
                        String abbreviationName = name.Replace("Saint ", "ST ");
                        AddTypeGuesser(CityDictionaryDescription, abbreviationName, allowTextSplit);
                        abbreviationName = name.Replace("Saint ", "ST. ");
                        AddTypeGuesser(CityDictionaryDescription, abbreviationName, allowTextSplit);
                    }

                    if (name.Contains("Fort "))
                    {
                        String abbreviationName = name.Replace("Fort ", "FT ");
                        AddTypeGuesser(CityDictionaryDescription, abbreviationName, allowTextSplit);
                        abbreviationName = name.Replace("Fort ", "FT. ");
                        AddTypeGuesser(CityDictionaryDescription, abbreviationName, allowTextSplit);
                    }
                }
            }
        }

        if (_value2GuessersDictionary.ContainsKey("apt"))
        {
            _value2GuessersDictionary.Remove("apt");
        }

        if (_value2GuessersDictionary.ContainsKey("highway"))
        {
            _value2GuessersDictionary.Remove("highway");
        }

        OnProgressChanged?.Invoke($"Dictionary records read: {recordCounter}");

        OnLoadingProfilerDictionaries("Geo Data");
    }

    private async Task LoadCommonNamesFromResourceAsync(Dictionary<string, string> dictionary)
    {
        await Task.Run(() =>
        {
            try
            {
                var table = new DataTable("CommonNames");

                // Use legacy ResourceHelper to load encrypted/compressed resource
                if (!ResourceHelper.LoadTableFromResourcesXml(table, CommonNamesResourceName))
                {
                    throw new InvalidOperationException(
                        $"Cannot find or load resource: {CommonNamesResourceName}");
                }

                // Parse the DataTable into dictionary
                foreach (DataRow row in table.Rows)
                {
                    try
                    {
                        if (row["FirstName"] != DBNull.Value && row["Form Of"] != DBNull.Value)
                        {
                            var firstName = row["FirstName"].ToString().Trim();
                            var formOf = row["Form Of"].ToString().Trim();

                            if (!string.IsNullOrEmpty(firstName) && !string.IsNullOrEmpty(formOf))
                            {
                                // Use case-insensitive key
                                dictionary[firstName] = formOf;
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        //_logger.LogWarning(ex, "Error parsing common name row");
                    }
                }

                //_logger.LogDebug("Loaded {Count} common names from resource", dictionary.Count);
            }
            catch (Exception ex)
            {
                //_logger.LogError(ex, "Error loading common names from resource: {ResourceName}",
                //    CommonNamesResourceName);
                throw;
            }
        });
    }
    private void LoadCommonNames()
    {
        if (!DictionaryOptions?.IsEnabled(FirstNameDictionaryDesription) ?? false) return;
        var commonNames = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        LoadCommonNamesFromResourceAsync(commonNames).Wait();

        Boolean allowTextSplit = false;

        foreach (KeyValuePair<String, String> keyValuePair in commonNames)
        {
            String firstName = keyValuePair.Key;
            String formOf = keyValuePair.Value;
            AddTypeGuesser(FirstNameDictionaryDesription, firstName, allowTextSplit);
            AddTypeGuesser(FirstNameDictionaryDesription, formOf, allowTextSplit);
        }

        _commonNamesDictionary = commonNames;
        OnLoadingProfilerDictionaries("Common Names");
    }

    private void LoadLastNames()
    {
        if (!DictionaryOptions?.IsEnabled(LastNameDictionaryDesription) ?? false) return;

        var fn = "MatchLogic.Infrastructure.Resources.last names only.archive";
        var assembly = System.Reflection.Assembly.GetExecutingAssembly();            

        OnProgressChanged?.Invoke("Opening dictionary...");

        string[] resources = assembly.GetManifestResourceNames();
        if (!resources.Contains(fn))
        {
            return;
        }
        using (Stream encryptedCompressedStream = assembly.GetManifestResourceStream(fn))
        using (MemoryStream decryptedCompressedStream = EncryptDecryptString.DecryptStream(encryptedCompressedStream))
        //using (MemoryStream decompressedStream = CompressWrapper.Decompress(decryptedCompressedStream))
        using (StreamReader streamReader = new StreamReader(decryptedCompressedStream))
        using (TextFieldParser reader = new TextFieldParser(streamReader))
        {
            reader.SetDelimiters("\t");
            reader.CommentTokens = new[] { "#" };
            reader.HasFieldsEnclosedInQuotes = false;

            if (reader.EndOfData)
            {
                return;
            }

            String[] values = reader.ReadFields();

            while (!reader.EndOfData)
            {
                values = reader.ReadFields();
                String lastName = (values?.Length ?? 0) > 0 ? values[0] : String.Empty;

                AddTypeGuesser(LastNameDictionaryDesription, lastName, false);
            }
        }
        

        OnLoadingProfilerDictionaries("Last Names");
    }
    
    private void LoadUserDefinedDictionaries()
    {
        Boolean allowTextSplit = false; // for now...
        //for (Int32 i = 0; i < StringDataTypeDeterminator.RecordCount; i++)
        //{
        //    RegexTypeGuesser stringTypeGuesser = StringDataTypeDeterminator[i];
        //    if (File.Exists(stringTypeGuesser.WordSmithFullFileName))
        //    {
        //        try
        //        {
        //            DataTable dataTable = new DataTable();
        //            dataTable.ReadXml(PathValidator.Simple.SanitizePath(stringTypeGuesser.WordSmithFullFileName));
        //            for (Int32 rowIndex = 0; rowIndex < dataTable.Rows.Count; rowIndex++)
        //            {
        //                DataRow dataRow = dataTable.Rows[rowIndex];
        //                String typeName = stringTypeGuesser.Description;
        //                Object obj = dataRow[(Int32)EditNames.Word];
        //                String value = (obj == DBNull.Value) ? "" : (String)obj;
        //                AddTypeGuesser(typeName, value, allowTextSplit);
        //            }
        //        }
        //        catch
        //        {
        //            /* do nothing... */
        //        }
        //    }
        //}

        OnLoadingProfilerDictionaries("User Defined");
    }
    
    private void AddTypeGuesser(String typeName, String value, Boolean allowTextSplit)
    {
        value = value.Trim().ToLower();

        if (String.IsNullOrEmpty(value))
        {
            return;
        }

        Dictionary<String, DictionaryTypeGuesser> allDictionariesForValue;

        if (!_value2GuessersDictionary.ContainsKey(value))
        {
            allDictionariesForValue = new Dictionary<String, DictionaryTypeGuesser>();
            _value2GuessersDictionary.Add(value, allDictionariesForValue);
        }
        else
        {
            allDictionariesForValue = _value2GuessersDictionary[value];
        }

        DictionaryTypeGuesser dictionaryTypeGuesser;

        if (!allDictionariesForValue.ContainsKey(typeName))
        {
            dictionaryTypeGuesser = new DictionaryTypeGuesser(typeName, this, allowTextSplit);
            allDictionariesForValue.Add(typeName, dictionaryTypeGuesser);
        }
    }

    /// <summary>
    /// Looks for dictionaries containing the tested data. Returns the list of dictionary descriptions.
    /// </summary>
    /// <param name="testString">String data to test.</param>
    /// <returns></returns>
    public List<String> FindTypeNames(String testString)
    {
        List<String> result = new List<String>();
        Dictionary<String, DictionaryTypeGuesser> allTypesForValue = null;

        testString = testString.Trim().ToLower();
        _value2GuessersDictionary.TryGetValue(testString, out allTypesForValue);

        if (allTypesForValue != null)
        {
            foreach (KeyValuePair<String, DictionaryTypeGuesser> keyValuepair in allTypesForValue)
            {
                result.Add(keyValuepair.Key);
            }
        }

        return result;
    }

    /// <summary>
    /// Looks for dictionaries containing the tested data. Returns the list of dictionary type guessers.
    /// </summary>
    /// <param name="testString">String data to test.</param>
    /// <returns></returns>
    public List<DictionaryTypeGuesser> FindTypes(String testString)
    {
        List<DictionaryTypeGuesser> result = new List<DictionaryTypeGuesser>();
        Dictionary<String, DictionaryTypeGuesser> allTypesForValue = null;

        testString = testString.Trim().ToLower();
        _value2GuessersDictionary.TryGetValue(testString, out allTypesForValue);

        if (allTypesForValue != null)
        {
            foreach (KeyValuePair<String, DictionaryTypeGuesser> keyValuepair in allTypesForValue)
            {
                DictionaryTypeGuesser stringPredefinedTypeGuesser = keyValuepair.Value;
                result.Add(stringPredefinedTypeGuesser);
            }
        }

        String[] values = testString.Split(new Char[] { ' ', '\t' });

        if (values.Length > 1)
        {
            for (Int32 i = 0; i < values.Length; i++)
            {
                String value = values[i];
                _value2GuessersDictionary.TryGetValue(value, out allTypesForValue);

                if (allTypesForValue != null)
                {
                    foreach (KeyValuePair<String, DictionaryTypeGuesser> keyValuepair in allTypesForValue)
                    {
                        DictionaryTypeGuesser stringPredefinedTypeGuesser = keyValuepair.Value;
                        if (stringPredefinedTypeGuesser.AllowTextSplit)
                        {
                            result.Add(stringPredefinedTypeGuesser);
                        }
                    }
                }
            }

        }

        return result;
    }

    /// <summary>
    /// Clears the internal set of type guessers.
    /// </summary>
    public void Clear()
    {
        _value2GuessersDictionary.Clear();
    }

    #endregion

    #region Delegates and Handlers
    
    public delegate void OnProgressChangedDelegate(String message);

    public OnProgressChangedDelegate OnProgressChanged { get; set; }

    #endregion
}