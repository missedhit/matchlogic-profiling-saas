using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.CleansingAndStandardization.Parser.GenericParser
{
    /// <summary>
    ///
    /// </summary>
    internal class RecognizedPatterns
    {
        #region Fields

        private Dictionary<Int32, RecognizedPattern> internalRecognizedPatternDictionary =
            new Dictionary<Int32, RecognizedPattern>();

        private List<RecognizedPattern> internalRecognizedPatternList = new List<RecognizedPattern>();

        #endregion

        #region Indexers

        internal RecognizedPattern this[Int32 patternId]
        {
            get
            {
                RecognizedPattern result;
                internalRecognizedPatternDictionary.TryGetValue(patternId, out result);
                return result;
            }
        }

        #endregion

        #region Properties and Fields

        public Int32 Count
        {
            get { return internalRecognizedPatternList.Count; }
        }

        #endregion

        #region Methods

        public void AddIfNeeded(RecognizedPattern recognizedPattern)
        {
            if (!internalRecognizedPatternDictionary.ContainsKey(recognizedPattern.PatternsGroup.PatternId))
            {
                internalRecognizedPatternDictionary.Add(recognizedPattern.PatternsGroup.PatternId, recognizedPattern);
                internalRecognizedPatternList.Add(recognizedPattern);
            }
            else
            {
                for (Int32 i = 0; i < recognizedPattern.Count; i++)
                {
                    internalRecognizedPatternDictionary[recognizedPattern.PatternsGroup.PatternId]
                        .AddCategorizedGroupOfWordsList(recognizedPattern[i]);
                }

            }
        }

        /// <summary>
        /// add recognized pattern to dictionary if pattern not already there. in that case return the existing recognized pattern
        /// </summary>
        /// <param name="patternsGroup"></param>
        /// <returns></returns>
        public RecognizedPattern AddIfNeeded(PatternsGroup patternsGroup)
        {
            RecognizedPattern result = new RecognizedPattern(patternsGroup);
            if (!internalRecognizedPatternDictionary.ContainsKey(patternsGroup.PatternId))
            {
                internalRecognizedPatternDictionary.Add(patternsGroup.PatternId, result);
                internalRecognizedPatternList.Add(result);
            }
            else
            {
                result = internalRecognizedPatternDictionary[patternsGroup.PatternId];
            }

            return result;
        }

        public void Clear()
        {
            internalRecognizedPatternDictionary.Clear();
            internalRecognizedPatternList.Clear();
        }

        #endregion
    }

    /// <summary>
    /// here we associate the pattern and all word groups which matched that pattern (for later use)
    /// </summary>
    internal class RecognizedPattern
    {
        #region Fields

        internal readonly PatternsGroup PatternsGroup;

        /// <summary>
        /// allows more strings to be recognized as same pattern from the input string
        /// e.g. if we have "New York" and "Los Angeles" both in the address. It has no much sense, but we leave to higher logic to work with it later
        /// </summary>
        internal readonly List<CategorizedGroupOfWordsList> categorizedGroupOfWordsListList =
            new List<CategorizedGroupOfWordsList>();

        private CategorizedGroupOfWordsList activeCategorizedGroupOfWordsList;

        #endregion

        #region Indexers

        internal CategorizedGroupOfWordsList this[Int32 index]
        {
            get { return categorizedGroupOfWordsListList[index]; }
        }

        #endregion

        #region Constructors

        internal RecognizedPattern(PatternsGroup patternsGroup)
        {
            PatternsGroup = patternsGroup;
            addNewCategorizedGroupOfWordsList();
        }

        #endregion

        #region Properties and Fields

        public Int32 Count
        {
            get { return categorizedGroupOfWordsListList.Count; }
        }

        private Boolean lockedActiveGroup = false;

        /// <summary>
        /// when set to true causes the next AdWordsIfNeeded to create a new group
        /// </summary>
        public Boolean LockedActiveGroup
        {
            get { return lockedActiveGroup; }
            set { lockedActiveGroup = value; }
        }

        #endregion

        #region Methods

        private void addNewCategorizedGroupOfWordsList()
        {
            activeCategorizedGroupOfWordsList = new CategorizedGroupOfWordsList();
            categorizedGroupOfWordsListList.Add(activeCategorizedGroupOfWordsList);
        }

        public void AdWordsIfNeeded(CategorizedGroupOfWords categorizedGroupOfWords)
        {
            if (!activeCategorizedGroupOfWordsList.Contains(categorizedGroupOfWords))
            {
                if (LockedActiveGroup)
                {
                    LockedActiveGroup = false;
                    addNewCategorizedGroupOfWordsList();
                }

                activeCategorizedGroupOfWordsList.AddIfNeeded(categorizedGroupOfWords);
            }
        }

        public void AddCategorizedGroupOfWordsList(CategorizedGroupOfWordsList categorizedGroupOfWordsList)
        {
            categorizedGroupOfWordsListList.Add(categorizedGroupOfWordsList);
        }

        #endregion
    }
}
